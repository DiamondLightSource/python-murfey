from pathlib import Path

import pytest
from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.workflows.clem.register_align_and_merge_results import run
from tests.conftest import ExampleVisit, get_or_create_db_entry

session_id = ExampleVisit.murfey_session_id + 1
visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
processed_dir_name = "processed"
project_name = "Grid_1"


@pytest.mark.parametrize(
    "test_params",
    [  # Registered data | Incoming data
        ["_Lng_LVCC", ""],
        ["_Lng_LVCC", "_Lng_LVCC"],
        ["", ""],
        ["", "_Lng_LVCC"],
    ],
)
def test_run(
    test_params: tuple[str, str], murfey_db_session: SQLModelSession, tmp_path: Path
):
    # Unpack test params
    registered_type, incoming_type = test_params

    # Create a session to insert for this test
    murfey_session: MurfeyDB.Session = get_or_create_db_entry(
        murfey_db_session,
        MurfeyDB.Session,
        lookup_kwargs={
            "id": session_id,
            "name": visit_name,
            "visit": visit_name,
            "instrument_name": ExampleVisit.instrument_name,
        },
    )

    # Create an ImagingSite entry using the existing values
    registered_position_name = "Position_1" + registered_type
    image_path = (
        tmp_path
        / visit_name
        / "processed"
        / project_name
        / "TileScan_1"
        / registered_position_name
        / "*.tiff"
    )
    registered_series_name = f"{project_name}--TileScan_1--{registered_position_name}"
    site_name = registered_series_name.rstrip(registered_type)
    get_or_create_db_entry(
        murfey_db_session,
        MurfeyDB.ImagingSite,
        lookup_kwargs={
            "session_id": murfey_session.id,
            "site_name": site_name,
            "image_path": str(image_path),
        },
    )

    # Create the incoming message
    incoming_position_name = "Position_1" + incoming_type
    incoming_series_name = f"{project_name}--TileScan_1--{incoming_position_name}"
    # The site names should match
    assert site_name == incoming_series_name.rstrip(incoming_type)

    message = {
        "session_id": murfey_session.id,
        "result": {
            "series_name": incoming_series_name,
            "image_stacks": [],
            "align_self": False,
            "flatten": True,
            "align_across": False,
            "output_file": tmp_path / "dummy",
            "thumbnail": None,
            "thumbnail_size": None,
        },
    }

    # Run the function and check that the expected values were created
    result = run(message, murfey_db_session)
    assert result["success"]

    imaging_site = murfey_db_session.exec(
        select(MurfeyDB.ImagingSite)
        .where(MurfeyDB.ImagingSite.session_id == murfey_session.id)
        .where(
            MurfeyDB.ImagingSite.site_name == incoming_series_name.rstrip(incoming_type)
        )
    ).one()

    # Check that 'composite_created' is updated correctly
    if (
        # Both data types match
        (registered_type and incoming_type)
        or (not registered_type and not incoming_type)
        # Registered type is raw data, while incoming is denoised
        or (not registered_type and incoming_type)
    ):
        assert imaging_site.composite_created
    else:
        assert not imaging_site.composite_created
