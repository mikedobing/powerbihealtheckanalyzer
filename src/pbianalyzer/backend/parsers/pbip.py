"""Parse PBIP (Power BI Project) ZIP archives.

A PBIP project is a folder structure::

    Project/
    ├── ProjectName.pbip
    ├── ProjectName.SemanticModel/
    │   ├── model.bim              (TMSL - JSON)
    │   ├── definition.pbism
    │   └── definition/            (TMDL folder - alternative to model.bim)
    │       ├── database.tmdl
    │       ├── model.tmdl
    │       ├── relationships.tmdl
    │       ├── dataSources.tmdl
    │       ├── expressions.tmdl
    │       └── tables/
    │           ├── Sales.tmdl
    │           └── Product.tmdl
    └── ProjectName.Report/
        └── definition.pbir

Users upload the project folder as a ZIP.  This module detects the structure,
chooses model.bim (TMSL) or definition/ (TMDL), and assembles a PBIModel.
"""

from __future__ import annotations

import io
import logging
import zipfile

from .bim import parse_bim
from .models import ModelAnnotation, PBIModel
from .tmdl import (
    parse_tmdl_database,
    parse_tmdl_datasources,
    parse_tmdl_model,
    parse_tmdl_relationships,
    parse_tmdl_table,
)

logger = logging.getLogger(__name__)


def _find_semantic_model_root(names: list[str]) -> str | None:
    """Find the SemanticModel folder prefix inside a ZIP.

    Looks for paths like ``Foo.SemanticModel/model.bim`` or
    ``Foo.SemanticModel/definition/tables/``.  Returns the prefix
    (e.g. ``Foo.SemanticModel/``) or *None*.
    """
    for name in names:
        lower = name.lower()
        if ".semanticmodel/" in lower:
            idx = lower.index(".semanticmodel/")
            return name[: idx + len(".SemanticModel/")]
    return None


def _find_definition_root(names: list[str], sm_root: str) -> str | None:
    """Find the TMDL definition/ folder under the SemanticModel root."""
    prefix = (sm_root + "definition/").lower()
    for name in names:
        if name.lower().startswith(prefix):
            return sm_root + "definition/"
    return None


def _read_zip_text(zf: zipfile.ZipFile, path: str) -> str | None:
    """Read a text file from the ZIP, trying common encodings."""
    try:
        raw = zf.read(path)
    except KeyError:
        return None
    for enc in ("utf-8-sig", "utf-8", "utf-16-le"):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, ValueError):
            continue
    return raw.decode("utf-8", errors="replace")


def _find_case_insensitive(zf: zipfile.ZipFile, prefix: str, target: str) -> str | None:
    """Find a file in the ZIP using case-insensitive matching."""
    target_lower = (prefix + target).lower()
    for name in zf.namelist():
        if name.lower() == target_lower:
            return name
    return None


def parse_pbip_zip(content: bytes) -> PBIModel:
    """Parse a ZIP file containing a PBIP project folder.

    Detects whether the semantic model uses TMSL (model.bim) or TMDL
    (definition/ folder) and parses accordingly.
    """
    warnings: list[str] = []

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()

        # --- Locate the SemanticModel root -----------------------------------
        sm_root = _find_semantic_model_root(names)

        if sm_root is None:
            # Fallback: maybe the ZIP is just the definition folder itself,
            # or contains .tmdl files at the root.
            tmdl_files = [n for n in names if n.lower().endswith(".tmdl")]
            bim_files = [n for n in names if n.lower().endswith(".bim")]

            if bim_files:
                text = _read_zip_text(zf, bim_files[0])
                if text:
                    model = parse_bim(text)
                    model.parse_warnings = [
                        "Parsed model.bim from ZIP (no standard PBIP "
                        "folder structure detected)."
                    ]
                    return model

            if tmdl_files:
                sm_root = ""
                # Try to detect definition/ prefix
                for n in tmdl_files:
                    if "definition/" in n.lower():
                        idx = n.lower().index("definition/")
                        sm_root = n[:idx]
                        break
            else:
                return PBIModel(
                    name="(no PBIP structure found)",
                    parse_warnings=[
                        "ZIP does not contain a recognisable PBIP project "
                        "structure (no .SemanticModel/ folder, model.bim, "
                        "or .tmdl files found)."
                    ],
                )

        # --- Path 1: TMSL (model.bim) ----------------------------------------
        bim_path = _find_case_insensitive(zf, sm_root, "model.bim")
        if bim_path:
            text = _read_zip_text(zf, bim_path)
            if text:
                model = parse_bim(text)
                model.parse_warnings = [
                    "Parsed from PBIP project (model.bim / TMSL format)."
                ]
                return model

        # --- Path 2: TMDL (definition/ folder) --------------------------------
        def_root = _find_definition_root(names, sm_root)
        if def_root is None:
            # Maybe tmdl files at the SemanticModel root directly
            def_root = sm_root

        # database.tmdl
        db_path = _find_case_insensitive(zf, def_root, "database.tmdl")
        db_info = None
        if db_path:
            db_text = _read_zip_text(zf, db_path)
            if db_text:
                db_info = parse_tmdl_database(db_text)

        # model.tmdl
        model_path = _find_case_insensitive(zf, def_root, "model.tmdl")
        model_info = None
        if model_path:
            model_text = _read_zip_text(zf, model_path)
            if model_text:
                model_info = parse_tmdl_model(model_text)

        # relationships.tmdl
        rel_path = _find_case_insensitive(zf, def_root, "relationships.tmdl")
        relationships = []
        if rel_path:
            rel_text = _read_zip_text(zf, rel_path)
            if rel_text:
                relationships = parse_tmdl_relationships(rel_text)

        # dataSources.tmdl
        ds_path = _find_case_insensitive(zf, def_root, "dataSources.tmdl")
        if ds_path is None:
            ds_path = _find_case_insensitive(zf, def_root, "datasources.tmdl")
        data_sources = []
        if ds_path:
            ds_text = _read_zip_text(zf, ds_path)
            if ds_text:
                data_sources = parse_tmdl_datasources(ds_text)

        # tables/*.tmdl
        tables_prefix = (def_root + "tables/").lower()
        table_files = sorted(
            n for n in names
            if n.lower().startswith(tables_prefix)
            and n.lower().endswith(".tmdl")
        )

        tables = []
        for tf in table_files:
            table_text = _read_zip_text(zf, tf)
            if table_text:
                table = parse_tmdl_table(table_text)
                tables.append(table)

        if not tables and not table_files:
            # Maybe tables are at def_root directly (non-standard layout)
            for n in names:
                if (
                    n.lower().startswith(def_root.lower())
                    and n.lower().endswith(".tmdl")
                    and "/tables/" not in n.lower()
                    and n.lower() not in {
                        (def_root + f).lower()
                        for f in [
                            "database.tmdl", "model.tmdl",
                            "relationships.tmdl", "dataSources.tmdl",
                            "datasources.tmdl", "expressions.tmdl",
                            "functions.tmdl",
                        ]
                    }
                ):
                    table_text = _read_zip_text(zf, n)
                    if table_text:
                        table = parse_tmdl_table(table_text)
                        if table.name and table.name != "(unknown)":
                            tables.append(table)

        # Annotations from model metadata
        annotations = []
        if model_info and model_info.culture:
            annotations.append(
                ModelAnnotation(name="culture", value=model_info.culture)
            )

        warnings.append(
            "Parsed from PBIP project (TMDL format). "
            f"{len(tables)} table(s), {len(relationships)} relationship(s) extracted."
        )

        return PBIModel(
            name=db_info.name if db_info else "",
            compatibility_level=db_info.compatibility_level if db_info else 0,
            tables=tables,
            relationships=relationships,
            data_sources=data_sources,
            annotations=annotations,
            parse_warnings=warnings,
        )
