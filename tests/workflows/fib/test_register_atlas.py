from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.workflows.fib.register_atlas import FIBAtlasMetadata, run

session_id = 10
visit_name = "cm12345-6"
instrument_name = "test_instrument"


@pytest.fixture
def visit_dir(tmp_path: Path):
    visit_dir = tmp_path / "data/2020" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


def test_parse_metadata():
    pass


def test_register_fib_imaging_site():
    pass


def test_run_with_db(
    mocker: MockerFixture,
    visit_dir: Path,
    murfey_db_session: Session,
):
    test_file = (
        visit_dir / "maps/LayersData/Layer/Electron Snapshot/Electron Snapshot.tiff"
    )

    # Add a test visit to the database
    if not (
        session_entry := murfey_db_session.exec(
            select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
        ).one_or_none()
    ):
        session_entry = MurfeyDB.Session(id=session_id)
    session_entry.name = visit_name
    session_entry.visit = visit_name
    session_entry.instrument_name = instrument_name

    murfey_db_session.add(session_entry)
    murfey_db_session.commit()

    # Mock the metadata returned from the image file
    mock_metadata = FIBAtlasMetadata(
        visit_name=visit_name,
        file=test_file,
        voltage=2000,
        shift_x=0,
        shift_y=0,
        len_x=0.003072,
        len_y=0.002048,
        pos_x=0.003,
        pos_y=0.0003,
        pos_z=0.01,
        rotation=-1.309,
        tilt_alpha=0.8,
        tilt_beta=0,
        pixels_x=3072,
        pixels_y=2048,
        pixel_size_x=1e-6,
        pixel_size_y=1e-6,
    )
    mocker.patch(
        "murfey.workflows.fib.register_atlas._parse_metadata",
        return_value=mock_metadata,
    )

    # Run the function and check that it's run through to completion
    assert run(
        session_id=session_id,
        file=test_file,
        murfey_db=murfey_db_session,
    )
