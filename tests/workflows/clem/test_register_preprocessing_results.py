from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import ispyb.sqlalchemy as ISPyBDB
import pytest
from pytest_mock import MockerFixture
from sqlalchemy import select as sa_select
from sqlalchemy.orm.session import Session as SQLAlchemySession
from sqlmodel import select as sm_select
from sqlmodel.orm.session import Session as SQLModelSession

import murfey.util.db as MurfeyDB
from murfey.workflows.clem.register_preprocessing_results import (
    _register_clem_image_series,
    _register_dcg_and_atlas,
    _register_grid_square,
    run,
)
from tests.conftest import ExampleVisit, get_or_create_db_entry

visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
processed_dir_name = "processed"
grid_name = "Grid_1"
colors = ("gray", "green", "red")


@pytest.fixture
def rsync_basepath(tmp_path: Path):
    return tmp_path / "data"


def generate_preprocessing_messages(
    rsync_basepath: Path,
    session_id: int,
):
    # Make directory to where data for current grid is stored
    visit_dir = rsync_basepath / "2020" / visit_name
    processed_dir = visit_dir / processed_dir_name
    grid_dir = processed_dir / grid_name
    grid_dir.mkdir(parents=True, exist_ok=True)

    # Construct all the datasets to be tested
    datasets: list[tuple[Path, bool, bool, tuple[int, int], float, list[float]]] = [
        (
            grid_dir / "Overview_1" / "Image_1",
            False,
            True,
            (2400, 2400),
            1e-6,
            [0.002, 0.0044, 0.002, 0.0044],
        )
    ]
    # Add on metadata for a few grid squares
    datasets.extend(
        [
            (
                grid_dir / "TileScan_1" / f"Position_{n}",
                True,
                False,
                (2048, 2048),
                1.6e-7,
                [0.003, 0.00332768, 0.003, 0.00332768],
            )
            for n in range(5)
        ]
    )

    messages: list[dict[str, Any]] = []
    for dataset in datasets:
        # Unpack items from list of dataset parameters
        series_path = dataset[0]
        series_name = str(series_path.relative_to(processed_dir)).replace("/", "--")
        metadata = series_path / "metadata" / f"{series_path.stem}.xml"
        metadata.parent.mkdir(parents=True, exist_ok=True)
        metadata.touch(exist_ok=True)
        output_files = {color: str(series_path / f"{color}.tiff") for color in colors}
        for output_file in output_files.values():
            Path(output_file).touch(exist_ok=True)
        thumbnails = {
            color: str(series_path / ".thumbnails" / f"{color}.png") for color in colors
        }
        for v in thumbnails.values():
            if not (thumbnail := Path(v)).parent.exists():
                thumbnail.parent.mkdir(parents=True)
            thumbnail.touch(exist_ok=True)
        thumbnail_size = (512, 512)
        is_stack = dataset[1]
        is_montage = dataset[2]
        shape = dataset[3]
        pixel_size = dataset[4]
        extent = dataset[5]

        message = {
            "session_id": session_id,
            "result": {
                "series_name": series_name,
                "number_of_members": 3,
                "is_stack": is_stack,
                "is_montage": is_montage,
                "output_files": output_files,
                "thumbnails": thumbnails,
                "thumbnail_size": thumbnail_size,
                "metadata": str(metadata),
                "parent_lif": None,
                "parent_tiffs": {},
                "pixels_x": shape[0],
                "pixels_y": shape[1],
                "units": "m",
                "pixel_size": pixel_size,
                "resolution": 1 / pixel_size,
                "extent": extent,
            },
        }
        messages.append(message)
    return messages


@pytest.mark.skip
def test_register_clem_image_series():
    assert _register_clem_image_series


@pytest.mark.skip
def test_register_dcg_and_atlas():
    assert _register_dcg_and_atlas


@pytest.mark.skip
def test_register_grid_square():
    assert _register_grid_square


def test_run(
    mocker: MockerFixture,
    rsync_basepath: Path,
):
    # Mock the MurfeyDB connection
    mock_murfey_session_entry = MagicMock()
    mock_murfey_session_entry.instrument_name = ExampleVisit.instrument_name
    mock_murfey_session_entry.visit = visit_name
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec().return_value.one.return_value = mock_murfey_session_entry

    # Mock the registration helper functions
    mock_register_clem_series = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results._register_clem_image_series"
    )
    mock_register_dcg_and_atlas = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results._register_dcg_and_atlas"
    )
    mock_register_grid_square = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results._register_grid_square"
    )

    # Mock the align and merge workflow call
    mock_align_and_merge_call = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results.submit_cluster_request"
    )

    preprocessing_messages = generate_preprocessing_messages(
        rsync_basepath=rsync_basepath,
        session_id=ExampleVisit.murfey_session_id,
    )
    for message in preprocessing_messages:
        result = run(
            message=message,
            murfey_db=mock_murfey_db,
        )
        assert result == {"success": True}
    assert mock_register_clem_series.call_count == len(preprocessing_messages)
    assert mock_register_dcg_and_atlas.call_count == len(preprocessing_messages)
    assert mock_register_grid_square.call_count == len(preprocessing_messages)
    assert mock_align_and_merge_call.call_count == len(preprocessing_messages) * len(
        colors
    )


test_matrix = (
    # Reverse order of list
    (False,),
    (True,),
)


@pytest.mark.parametrize("test_params", test_matrix)
def test_run_with_db(
    mocker: MockerFixture,
    rsync_basepath: Path,
    mock_ispyb_credentials,
    murfey_db_session: SQLModelSession,
    ispyb_db_session: SQLAlchemySession,
    test_params: tuple[bool],
):
    # Unpack test params
    (shuffle_message,) = test_params

    # Create a session to insert for this test
    murfey_session: MurfeyDB.Session = get_or_create_db_entry(
        murfey_db_session,
        MurfeyDB.Session,
        lookup_kwargs={
            "id": ExampleVisit.murfey_session_id + 1,
            "name": visit_name,
            "visit": visit_name,
            "instrument_name": ExampleVisit.instrument_name,
        },
    )

    # Mock the ISPyB connection where the TransportManager class is located
    mock_security_config = MagicMock()
    mock_security_config.ispyb_credentials = mock_ispyb_credentials
    mocker.patch(
        "murfey.server.ispyb.get_security_config",
        return_value=mock_security_config,
    )
    mocker.patch(
        "murfey.server.ispyb.ISPyBSession",
        return_value=ispyb_db_session,
    )

    # Mock the ISPYB connection when registering data collection group
    mocker.patch(
        "murfey.workflows.register_data_collection_group.ISPyBSession",
        return_value=ispyb_db_session,
    )

    # Mock out the machine config used in the helper sanitisation function
    mock_get_machine_config = mocker.patch("murfey.workflows.clem.get_machine_config")
    mock_machine_config = MagicMock()
    mock_machine_config.rsync_basepath = rsync_basepath
    mock_get_machine_config.return_value = {
        ExampleVisit.instrument_name: mock_machine_config,
    }

    # Mock the align and merge workflow call
    mock_align_and_merge_call = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results.submit_cluster_request"
    )

    # Patch the TransportManager object in the workflows called
    from murfey.server.ispyb import TransportManager

    mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results._transport_object",
        new=TransportManager("PikaTransport"),
    )
    mocker.patch(
        "murfey.workflows.register_data_collection_group._transport_object",
        new=TransportManager("PikaTransport"),
    )
    mocker.patch(
        "murfey.workflows.register_atlas_update._transport_object",
        new=TransportManager("PikaTransport"),
    )

    # Run the function
    preprocessing_messages = generate_preprocessing_messages(
        rsync_basepath=rsync_basepath,
        session_id=murfey_session.id,
    )
    if shuffle_message:
        preprocessing_messages.reverse()
    for message in preprocessing_messages:
        result = run(
            message=message,
            murfey_db=murfey_db_session,
        )
        assert result == {"success": True}

    # Each message should call the align-and-merge workflow thrice
    # if gray and colour channels are both present
    assert mock_align_and_merge_call.call_count == len(preprocessing_messages) * len(
        colors
    )

    # Both databases should have entries for data collection group, and grid squares
    # ISPyB database should additionally have an atlas entry
    murfey_dcg_search = murfey_db_session.exec(
        sm_select(MurfeyDB.DataCollectionGroup).where(
            MurfeyDB.DataCollectionGroup.session_id == murfey_session.id
        )
    ).all()
    assert len(murfey_dcg_search) == 1
    murfey_gs_search = murfey_db_session.exec(
        sm_select(MurfeyDB.GridSquare).where(
            MurfeyDB.GridSquare.session_id == murfey_session.id
        )
    ).all()
    assert len(murfey_gs_search) == len(preprocessing_messages) - 1

    murfey_dcg = murfey_dcg_search[0]
    ispyb_dcg_search = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.DataCollectionGroup).where(
                ISPyBDB.DataCollectionGroup.dataCollectionGroupId == murfey_dcg.id
            )
        )
        .scalars()
        .all()
    )
    assert len(ispyb_dcg_search) == 1

    ispyb_dcg = ispyb_dcg_search[0]
    ispyb_atlas_search = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.Atlas).where(
                ISPyBDB.Atlas.dataCollectionGroupId == ispyb_dcg.dataCollectionGroupId
            )
        )
        .scalars()
        .all()
    )
    assert len(ispyb_atlas_search) == 1

    ispyb_atlas = ispyb_atlas_search[0]
    ispyb_gs_search = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.GridSquare).where(
                ISPyBDB.GridSquare.atlasId == ispyb_atlas.atlasId
            )
        )
        .scalars()
        .all()
    )
    assert len(ispyb_gs_search) == len(preprocessing_messages) - 1

    murfey_db_session.close()
    ispyb_db_session.close()
