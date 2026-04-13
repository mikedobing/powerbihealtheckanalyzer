"""Parse .pbix files (Power BI Desktop ZIP archives).

Supports two internal formats:
- **DataModelSchema** (JSON): Older PBIX files and .pbit templates store the
  tabular model as a UTF-16LE JSON file.  Handled by `parse_bim()`.
- **DataModel** (binary ABF/XPress9): Modern PBIX files (post-2022) store the
  model as a compressed binary backup.  Handled by `pbixray`.
"""

from __future__ import annotations

import io
import json
import logging
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
from pbixray import PBIXRay

from .bim import parse_bim
from .models import (
    Column,
    CrossFilterDirection,
    DataSource,
    Measure,
    ModelAnnotation,
    PBIModel,
    Partition,
    Relationship,
    ReportLayout,
    ReportPage,
    ReportVisual,
    StorageMode,
    Table,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report/Layout parsing (shared by both paths)
# ---------------------------------------------------------------------------

def _parse_report_layout(layout_bytes: bytes) -> ReportLayout:
    """Parse the Report/Layout JSON from a PBIX file."""
    try:
        text = layout_bytes.decode("utf-16-le")
    except UnicodeDecodeError:
        text = layout_bytes.decode("utf-8-sig")

    data = json.loads(text)
    pages: list[ReportPage] = []
    for section in data.get("sections", []):
        visuals: list[ReportVisual] = []
        for vc in section.get("visualContainers", []):
            config_str = vc.get("config", "{}")
            try:
                config = json.loads(config_str)
                visual_type = (
                    config.get("singleVisual", {}).get("visualType", "")
                    or config.get("visualType", "")
                )
            except (json.JSONDecodeError, TypeError):
                visual_type = ""
            visuals.append(ReportVisual(visual_type=visual_type))
        pages.append(ReportPage(
            name=section.get("name", ""),
            display_name=section.get("displayName", ""),
            visuals=visuals,
        ))
    return ReportLayout(pages=pages)


# ---------------------------------------------------------------------------
# pbixray -> PBIModel mapping
# ---------------------------------------------------------------------------

_CARDINALITY_MAP = {
    "1:1": ("one", "one"),
    "1:M": ("one", "many"),
    "M:1": ("many", "one"),
    "M:M": ("many", "many"),
}

_CROSSFILTER_MAP = {
    "single": CrossFilterDirection.ONE,
    "both": CrossFilterDirection.BOTH,
    "automatic": CrossFilterDirection.AUTOMATIC,
}


def _safe_str(val: object) -> str:
    """Convert a value to str, treating pandas NA / None as empty string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if pd.isna(val):  # type: ignore[arg-type]
        return ""
    return str(val)


def _map_pbixray_to_model(ray: PBIXRay) -> tuple[PBIModel, list[str]]:
    """Map PBIXRay DataFrames to our PBIModel structure.

    Returns (model, warnings) where warnings note any data that couldn't be
    fully mapped.
    """
    warnings: list[str] = []
    table_names: list[str] = list(ray.tables)

    # -- Build column map: table_name -> [Column, ...] -----------------------
    col_map: dict[str, list[Column]] = {t: [] for t in table_names}
    try:
        schema_df = ray.schema
        if not schema_df.empty:
            for _, row in schema_df.iterrows():
                tname = str(row.get("TableName", ""))
                if tname in col_map:
                    col_map[tname].append(Column(
                        name=_safe_str(row.get("ColumnName")),
                        data_type=_safe_str(row.get("PandasDataType")),
                    ))
    except Exception as exc:
        logger.warning("pbixray schema extraction failed: %s", exc)
        warnings.append(f"Column schema extraction failed: {exc}")

    # -- Build measure map: table_name -> [Measure, ...] ---------------------
    measure_map: dict[str, list[Measure]] = {t: [] for t in table_names}
    try:
        dax_df = ray.dax_measures
        if not dax_df.empty:
            for _, row in dax_df.iterrows():
                tname = str(row.get("TableName", ""))
                if tname in measure_map:
                    measure_map[tname].append(Measure(
                        name=_safe_str(row.get("Name")),
                        expression=_safe_str(row.get("Expression")),
                        description=_safe_str(row.get("Description")),
                    ))
    except Exception as exc:
        logger.warning("pbixray DAX measures extraction failed: %s", exc)
        warnings.append(f"DAX measure extraction failed: {exc}")

    # -- Build calculated table set ------------------------------------------
    calc_tables: dict[str, str] = {}
    try:
        dax_tables_df = ray.dax_tables
        if not dax_tables_df.empty:
            for _, row in dax_tables_df.iterrows():
                calc_tables[str(row.get("TableName", ""))] = _safe_str(
                    row.get("Expression")
                )
    except Exception as exc:
        logger.warning("pbixray DAX tables extraction failed: %s", exc)

    # -- Build partition map from Power Query --------------------------------
    partition_map: dict[str, list[Partition]] = {t: [] for t in table_names}
    try:
        pq_df = ray.power_query
        if not pq_df.empty:
            for _, row in pq_df.iterrows():
                tname = str(row.get("TableName", ""))
                if tname in partition_map:
                    partition_map[tname].append(Partition(
                        name=tname,
                        source_type="m",
                        query=_safe_str(row.get("Expression")),
                    ))
    except Exception as exc:
        logger.warning("pbixray Power Query extraction failed: %s", exc)

    # -- Assemble Table objects -----------------------------------------------
    tables: list[Table] = []
    for tname in table_names:
        is_calc = tname in calc_tables
        calc_expr = calc_tables.get(tname)

        partitions = partition_map.get(tname, [])
        if is_calc and not partitions:
            partitions = [Partition(
                name=tname,
                source_type="calculated",
                query=calc_expr or "",
            )]

        tables.append(Table(
            name=tname,
            columns=col_map.get(tname, []),
            measures=measure_map.get(tname, []),
            partitions=partitions,
            storage_mode=StorageMode.DEFAULT,
            is_calculated=is_calc,
            calculated_table_expression=calc_expr,
        ))

    if not tables:
        warnings.append("No tables extracted from the binary DataModel")

    # -- Relationships -------------------------------------------------------
    relationships: list[Relationship] = []
    skipped_rels = 0
    try:
        rel_df = ray.relationships
        if not rel_df.empty:
            for _, row in rel_df.iterrows():
                from_tbl = _safe_str(row.get("FromTableName"))
                from_col = _safe_str(row.get("FromColumnName"))
                to_tbl = _safe_str(row.get("ToTableName"))
                to_col = _safe_str(row.get("ToColumnName"))

                if not from_tbl:
                    skipped_rels += 1
                    continue

                # pbixray may return None for auto-date-table targets;
                # include the relationship with a placeholder so rule
                # checks can still count it.
                if not to_tbl:
                    to_tbl = "(unresolved)"
                    to_col = to_col or "(unresolved)"
                    skipped_rels += 1

                card_str = _safe_str(row.get("Cardinality"))
                from_card, to_card = _CARDINALITY_MAP.get(
                    card_str, ("many", "one")
                )

                cf_raw = _safe_str(row.get("CrossFilteringBehavior")).lower()
                cross_dir = _CROSSFILTER_MAP.get(
                    cf_raw, CrossFilterDirection.ONE
                )

                is_active_val = row.get("IsActive")
                is_active = bool(is_active_val) if is_active_val is not None else True

                relationships.append(Relationship(
                    from_table=from_tbl,
                    from_column=from_col,
                    to_table=to_tbl,
                    to_column=to_col,
                    cross_filter_direction=cross_dir,
                    from_cardinality=from_card,
                    to_cardinality=to_card,
                    is_active=is_active,
                ))

            if skipped_rels:
                warnings.append(
                    f"{skipped_rels} relationship(s) have unresolved target "
                    "tables (typically auto-date-table references)."
                )
    except Exception as exc:
        logger.warning("pbixray relationship extraction failed: %s", exc)
        warnings.append(f"Relationship extraction failed: {exc}")

    # -- Metadata / annotations ----------------------------------------------
    annotations: list[ModelAnnotation] = []
    try:
        meta_df = ray.metadata
        if not meta_df.empty:
            for _, row in meta_df.iterrows():
                annotations.append(ModelAnnotation(
                    name=_safe_str(row.get("Name")),
                    value=_safe_str(row.get("Value")),
                ))
    except Exception as exc:
        logger.warning("pbixray metadata extraction failed: %s", exc)

    warnings.append(
        "Parsed from binary DataModel via pbixray. "
        "Storage modes are inferred (not explicitly stored in binary format). "
        "For the most complete analysis, export a .bim file from Tabular Editor."
    )

    model = PBIModel(
        name="",
        tables=tables,
        relationships=relationships,
        annotations=annotations,
        parse_warnings=warnings,
    )
    return model, warnings


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def parse_pbix(content: bytes) -> PBIModel:
    """Parse a .pbix or .pbit file and return a normalized PBIModel.

    Tries DataModelSchema (JSON) first, then falls back to the binary
    DataModel via pbixray.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()

        model: PBIModel | None = None

        # Path 1: JSON DataModelSchema (older PBIX, .pbit templates)
        for candidate in ["DataModelSchema", "dataModelSchema"]:
            if candidate in names:
                schema_bytes = zf.read(candidate)
                try:
                    schema_text = schema_bytes.decode("utf-16-le")
                except UnicodeDecodeError:
                    schema_text = schema_bytes.decode("utf-8-sig")
                model = parse_bim(schema_text)
                break

        # Path 2: Binary DataModel (modern PBIX) -- use pbixray
        if model is None and "DataModel" in names:
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=".pbix", delete=False
                ) as tmp:
                    tmp.write(content)
                    tmp_path = tmp.name

                ray = PBIXRay(tmp_path)
                model, _ = _map_pbixray_to_model(ray)
            except Exception as exc:
                logger.error("pbixray parsing failed: %s", exc)
                model = PBIModel(
                    name="(binary model – parse error)",
                    parse_warnings=[
                        f"Failed to parse binary DataModel: {exc}",
                        "Please export a .bim file from Tabular Editor for "
                        "full analysis.",
                    ],
                )
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        if model is None:
            model = PBIModel(
                name="(no model found)",
                parse_warnings=[
                    "No DataModelSchema or DataModel found in the PBIX file."
                ],
            )

        # Attach report layout (available in both old and new formats)
        for candidate in ["Report/Layout", "report/Layout"]:
            if candidate in names:
                layout_bytes = zf.read(candidate)
                model.report_layout = _parse_report_layout(layout_bytes)
                break

    return model
