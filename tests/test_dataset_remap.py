"""Multi-dataset build: `_remap_datasets` rewrites a view's qualified cross-dataset refs
(`ods.x`) to the sandbox build datasets (`dmt_build_ods.x`), so build-and-verify can stand up
cross-dataset views WITHOUT owning a real-named dataset (the `_guard` forbids those). Offline.
"""
from lib.build import _remap_datasets

MAP = {"staging": "dmt_build_staging", "ods": "dmt_build_ods", "dm": "dmt_build_dm"}


def test_remaps_qualified_ref():
    assert _remap_datasets("SELECT * FROM ods.ods_org_unit", MAP) == \
        "SELECT * FROM dmt_build_ods.ods_org_unit"   # dataset remapped, table name (ods_*) left alone


def test_remaps_multiple_layers():
    out = _remap_datasets("SELECT a.x FROM ods.t a JOIN staging.s b ON a.id = b.id", MAP)
    assert "dmt_build_ods.t" in out and "dmt_build_staging.s" in out
    assert "a.x" in out and "a.id" in out and "b.id" in out          # aliases untouched


def test_leaves_columns_and_substrings_untouched():
    out = _remap_datasets("SELECT ods_status, my_ods FROM dm.dim_x WHERE ods_status = 1", MAP)
    assert "ods_status" in out and "my_ods" in out                   # column / substring untouched
    assert "dmt_build_dm.dim_x" in out                               # the real ref IS remapped
    assert "dmt_build_ods_status" not in out                         # column not mangled


def test_handles_backtick_quoted_dataset():
    assert _remap_datasets("FROM `ods`.tbl", MAP) == "FROM dmt_build_ods.tbl"


def test_unmapped_refs_pass_through():
    assert _remap_datasets("FROM external_proj.tbl", MAP) == "FROM external_proj.tbl"
