"""Mode-2 build-and-verify spine (BQ-only): the harness applies the CUT's artifacts to
a clean build dataset, then the suite verifies the built schema."""
import pathlib

import pytest

from lib.report import Status

GOLDEN = pathlib.Path(__file__).parent / "build_and_verify.mvs.yaml"
MULTI = pathlib.Path(__file__).parent / "multi_dataset.mvs.yaml"


@pytest.mark.live_bq
def test_build_then_verify(run_spec_file, bq_engine):
    report = run_spec_file(GOLDEN)
    assert report.status == Status.PASS, "build/verify failures: " + "; ".join(
        f"{c.target}: {c.message}" for c in report.failures())
    # the build step must NOT have produced an error suite
    assert not any(s.pattern == "migration_build" for s in report.suites), \
        "build failed before verification"
    assert any(s.pattern == "schema_conformance" for s in report.suites)


@pytest.mark.live_bq
def test_multi_dataset_cross_view(run_spec_file, bq_engine):
    # a view in dmt_build_dm reads a table in dmt_build_ods via the remapped production ref
    # ods.org_unit -> dmt_build_ods.org_unit; both build datasets are provisioned + torn down.
    report = run_spec_file(MULTI)
    assert report.status == Status.PASS, "multi-dataset build/verify failures: " + "; ".join(
        f"{c.target}: {c.message}" for c in report.failures())
    assert not any(s.pattern == "migration_build" for s in report.suites), \
        "cross-dataset build failed before verification"
