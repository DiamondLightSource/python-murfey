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
    COLOR_FLAGS_MURFEY_TO_ISPYB,
    CLEMPreprocessingResult,
    _determine_collection_mode,
    _get_color_flags,
    _register_clem_imaging_site,
    _register_dcg_and_atlas,
    _register_grid_square,
    _snake_to_camel_case,
    run,
)
from tests.conftest import ExampleVisit

visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name
session_id = ExampleVisit.murfey_session_id
processed_dir_name = "processed"
grid_name = "Grid_1"


@pytest.fixture
def rsync_basepath(tmp_path: Path):
    return tmp_path / "data"


def generate_preprocessing_messages(
    session_id: int,
    processed_dir: Path,
    colors: list[str],
    datasets: list[tuple[Path, bool, bool, tuple[int, int], float, list[float]]],
):
    messages: list[dict[str, Any]] = []
    for series_path, is_stack, is_montage, shape, pixel_size, extent in datasets:
        # Unpack items from list of dataset parameters
        series_name = str(series_path.relative_to(processed_dir)).replace(" ", "_")
        metadata = series_path / "metadata" / f"{series_path.stem}.xml"
        metadata.parent.mkdir(parents=True, exist_ok=True)
        metadata.touch(exist_ok=True)
        output_files = {color: str(series_path / f"{color}.tiff") for color in colors}
        for output_file in output_files.values():
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            Path(output_file).touch(exist_ok=True)
        thumbnails = {
            color: str(series_path / ".thumbnails" / f"{color}.png") for color in colors
        }
        for v in thumbnails.values():
            if not (thumbnail := Path(v)).parent.exists():
                thumbnail.parent.mkdir(parents=True)
            thumbnail.touch(exist_ok=True)
        thumbnail_size = (512, 512)

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


@pytest.mark.parametrize(
    "test_params",
    (
        (
            ["gray"],
            {
                "has_grey": True,
            },
        ),
        (
            ["gray", "red"],
            {
                "has_grey": True,
                "has_red": True,
            },
        ),
        (
            ["red", "green", "blue"],
            {
                "has_red": True,
                "has_green": True,
                "has_blue": True,
            },
        ),
        (
            ["cyan", "magenta", "yellow"],
            {
                "has_cyan": True,
                "has_magenta": True,
                "has_yellow": True,
            },
        ),
    ),
)
def test_get_color_flags(test_params: tuple[list[str], dict[str, bool]]):
    colors, positive_flags = test_params
    expected_result = dict.fromkeys(
        (
            "has_grey",
            "has_red",
            "has_green",
            "has_blue",
            "has_cyan",
            "has_magenta",
            "has_yellow",
        ),
        False,
    )
    for flag, value in positive_flags.items():
        expected_result[flag] = value
    assert _get_color_flags(colors) == expected_result


def test_register_clem_imaging_site():
    _register_clem_imaging_site


@pytest.mark.parametrize(
    "test_params",
    (
        (["gray"], "Bright Field"),
        (["gray", "blue"], "Bright Field and Fluorescent"),
        (["red", "green", "blue"], "Fluorescent"),
    ),
)
def test_determine_collection_mode(test_params: tuple[list[str], str]):
    colors, expected_result = test_params
    assert _determine_collection_mode(colors) == expected_result


@pytest.mark.parametrize(
    "test_params",
    (
        ("has_grey", "hasGrey"),
        ("has_red", "hasRed"),
        ("has_green", "hasGreen"),
        ("has_blue", "hasBlue"),
        ("has_cyan", "hasCyan"),
        ("has_magenta", "hasMagenta"),
        ("has_yellow", "hasYellow"),
    ),
)
def test_snake_to_camel_case(
    test_params: tuple[str, str],
):
    string, expected_result = test_params
    assert _snake_to_camel_case(string) == expected_result


@pytest.mark.parametrize(
    "shuffle_message",
    (True, False),
)
def test_register_dcg_and_atlas(
    mocker: MockerFixture,
    shuffle_message: bool,
    rsync_basepath: Path,
    mock_ispyb_credentials: Path,
    ispyb_db_session: SQLAlchemySession,
    murfey_db_session: SQLModelSession,
):
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

    # Patch the TransportManager object in the workflows called
    from murfey.server.ispyb import TransportManager

    transport_object = TransportManager("PikaTransport")
    transport_object.feedback_queue = "murfey_feedback"
    mocker.patch("murfey.server._transport_object", new=transport_object)

    # Construct test datasets to use
    processed_dir = rsync_basepath / "2020" / visit_name / processed_dir_name
    grid_dir = processed_dir / grid_name
    datasets: list[tuple[Path, bool, bool, tuple[int, int], float, list[float]]] = [
        # Atlas image
        (
            grid_dir / "Overview 1" / "Image 1",
            False,
            True,
            (2400, 2400),
            1e-6,
            [0.002, 0.0044, 0.002, 0.0044],
        )
    ]
    # Add a slightly smaller TileScan
    datasets.extend(
        [
            (
                grid_dir / "TileScan 1",
                True,
                False,
                (2048, 2048),
                1.0e-6,
                [0.002, 0.004048, 0.002, 0.004048],
            )
        ]
    )
    preprocessing_messages = generate_preprocessing_messages(
        session_id=ExampleVisit.murfey_session_id,
        processed_dir=processed_dir,
        colors=["gray"],
        datasets=datasets,
    )
    if shuffle_message:
        preprocessing_messages.reverse()
    for message in preprocessing_messages:
        result = CLEMPreprocessingResult(**message["result"])
        # Register imaging site
        imaging_site = _register_clem_imaging_site(
            session_id=session_id,
            result=result,
            murfey_db=murfey_db_session,
        )
        # Register the DataCollecitonGroup and Atlas
        imaging_site = _register_dcg_and_atlas(
            session_id=session_id,
            instrument_name=instrument_name,
            visit_name=visit_name,
            imaging_site=imaging_site,
            transport_object=transport_object,
        )

    # Verify that the ImagingSite entries were created
    imaging_sites = murfey_db_session.exec(
        sm_select(MurfeyDB.ImagingSite).where(
            MurfeyDB.ImagingSite.session_id == session_id
        )
    ).all()
    assert len(imaging_sites) == 2
    for site in imaging_sites:
        # The "Overview" image should stay as an atlas
        if "Overview_1/Image_1" in site.site_name:
            assert site.data_type == "atlas"
        # "TileScan1" should be bumped down to grid square
        elif "TileScan_1" in site.site_name:
            assert site.data_type == "grid_square"

    # Murfey' DataCollectionGroup table should have 1 entry
    murfey_dcg_entry = murfey_db_session.exec(
        sm_select(MurfeyDB.DataCollectionGroup).where(
            MurfeyDB.DataCollectionGroup.session_id == 1
        )
    ).one()
    # "Overview" should be the registered atlas image
    assert "Overview_1" in murfey_dcg_entry.atlas

    # ISPyB's DataCollectionGroup table should have 1 entry
    assert (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.DataCollectionGroup).where(
                ISPyBDB.DataCollectionGroup.dataCollectionGroupId == murfey_dcg_entry.id
            )
        )
        .scalars()
        .one_or_none()
        is not None
    )

    # ISPyB's Atlas table should have 1 entry
    ispyb_atlas_entry = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.Atlas).where(
                ISPyBDB.Atlas.dataCollectionGroupId == murfey_dcg_entry.id
            )
        )
        .scalars()
        .one()
    )
    # The "Overview" file should be registered
    assert "Overview" in ispyb_atlas_entry.atlasImage


def test_register_grid_square():
    _register_grid_square


@pytest.mark.parametrize(
    "test_params",
    (
        # Reverse list order? | Colors | Denoising suffix
        (False, ["gray"], "_Lng_LVCC"),
        (True, ["gray"], "_Lng_SVCC"),
        (False, ["red", "green", "blue"], "_ICC"),
        (True, ["cyan", "magenta", "yellow"], "_Lng_LVCC"),
        (False, ["gray", "red", "green", "blue"], "_Lng_SVCC"),
        (True, ["gray", "cyan", "magenta", "yellow"], "_ICC"),
    ),
)
def test_run_with_db(
    mocker: MockerFixture,
    rsync_basepath: Path,
    mock_ispyb_credentials,
    murfey_db_session: SQLModelSession,
    ispyb_db_session: SQLAlchemySession,
    test_params: tuple[bool, list[str], str],
):
    # Unpack test params
    (shuffle_message, colors, denoising_suffix) = test_params

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

    # Patch the TransportManager object in the workflows called
    from murfey.server.ispyb import TransportManager

    mock_send = mocker.patch.object(TransportManager, "send")
    transport_object = TransportManager("PikaTransport")
    transport_object.feedback_queue = "murfey_feedback"
    mocker.patch("murfey.server._transport_object", new=transport_object)

    # Construct test datasets to use
    processed_dir = rsync_basepath / "2020" / visit_name / processed_dir_name
    grid_dir = processed_dir / grid_name
    datasets: list[tuple[Path, bool, bool, tuple[int, int], float, list[float]]] = [
        (
            grid_dir / "Overview 1" / "Image 1",
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
                grid_dir / "TileScan 1" / f"Position {n + 1}",
                True,
                False,
                (2048, 2048),
                1.6e-7,
                [0.003, 0.00332768, 0.003, 0.00332768],
            )
            for n in range(3)
        ]
    )
    # Add on metadata for denoised datasets
    datasets.extend(
        [
            (
                grid_dir / "TileScan 2" / f"Position {n + 1}_Lng_LVCC",
                True,
                False,
                (2048, 2048),
                1.6e-7,
                [0.003, 0.00332768, 0.003, 0.00332768],
            )
            for n in range(3)
        ]
    )
    # Add a large tile scan dataset
    datasets.extend(
        [
            (
                grid_dir / "TileScan 3",
                False,
                True,
                (2048, 2048),
                1e-6,
                [0.002, 0.004048, 0.002, 0.004048],
            )
        ]
    )
    preprocessing_messages = generate_preprocessing_messages(
        session_id=ExampleVisit.murfey_session_id,
        processed_dir=processed_dir,
        colors=colors,
        datasets=datasets,
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
    if ("gray" not in colors) or ("gray" in colors and len(colors) == 1):
        assert mock_send.call_count == len(preprocessing_messages)
    else:
        assert mock_send.call_count == len(preprocessing_messages) * 3

    # Murfey's DataCollectionGroup should have an entry
    murfey_dcg_search = murfey_db_session.exec(
        sm_select(MurfeyDB.DataCollectionGroup).where(
            MurfeyDB.DataCollectionGroup.session_id == session_id
        )
    ).all()
    assert len(murfey_dcg_search) == 1

    # GridSquare entries should be half the initial number of entries due to overwrites
    murfey_gs_search = murfey_db_session.exec(
        sm_select(MurfeyDB.GridSquare).where(
            MurfeyDB.GridSquare.session_id == session_id
        )
    ).all()
    assert len(murfey_gs_search) == (len(preprocessing_messages) - 2) // 2

    # ISPyB's DataCollectionGroup should have an entry
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

    # Atlas should have an entry
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

    # Determine the color flags and collection mode
    color_flags = {
        COLOR_FLAGS_MURFEY_TO_ISPYB[flag]: int(value)
        for flag, value in _get_color_flags(colors).items()
    }
    collection_mode = _determine_collection_mode(colors)

    # Atlas color flags and collection mode should be set correctly
    ispyb_atlas = ispyb_atlas_search[0]
    for flag, value in color_flags.items():
        assert getattr(ispyb_atlas, flag) == value
    assert ispyb_atlas.mode == collection_mode

    # ISPyB's GridSquare should have half the number of initial entries
    ispyb_gs_search = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.GridSquare).where(
                ISPyBDB.GridSquare.atlasId == ispyb_atlas.atlasId
            )
        )
        .scalars()
        .all()
    )
    assert len(ispyb_gs_search) == ((len(preprocessing_messages) - 2) // 2) + 1
    for gs in ispyb_gs_search:
        # Check that all entries point to the denoised images
        assert gs.gridSquareImage is not None and denoising_suffix in gs.gridSquareImage

        # Check that the GridSquare color flags and collection mode are set correctly
        for flag, value in color_flags.items():
            assert getattr(gs, flag) == value
        assert gs.mode == collection_mode

    murfey_db_session.close()
    ispyb_db_session.close()
