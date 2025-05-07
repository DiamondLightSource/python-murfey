from unittest import mock

from sqlmodel import Session, select

from murfey.util.db import DataCollectionGroup, GridSquare
from murfey.util.models import GridSquareParameters
from murfey.workflows.spa import flush_spa_preprocess
from tests.conftest import ExampleVisit


@mock.patch("murfey.workflows.spa.flush_spa_preprocess._transport_object")
def test_register_grid_square_update_add_locations(
    mock_transport, murfey_db_session: Session
):
    """Test the updating of an existing grid square"""
    # Create a grid square to update
    grid_square = GridSquare(
        id=1,
        name=101,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
    )
    murfey_db_session.add(grid_square)
    murfey_db_session.commit()

    # Parameters to update with
    new_parameters = GridSquareParameters(
        tag="session_tag",
        x_location=1.1,
        y_location=1.2,
        x_stage_position=1.3,
        y_stage_position=1.4,
    )

    # Run the registration
    flush_spa_preprocess.register_grid_square(
        ExampleVisit.murfey_session_id, 101, new_parameters, murfey_db_session
    )

    # Check this would have updated ispyb
    mock_transport.do_update_grid_square.assert_called_with(1, new_parameters)

    # Confirm the database was updated
    grid_square_final_parameters = murfey_db_session.exec(select(GridSquare)).one()
    assert grid_square_final_parameters.x_location == new_parameters.x_location
    assert grid_square_final_parameters.y_location == new_parameters.y_location
    assert (
        grid_square_final_parameters.x_stage_position == new_parameters.x_stage_position
    )
    assert (
        grid_square_final_parameters.y_stage_position == new_parameters.y_stage_position
    )


@mock.patch("murfey.workflows.spa.flush_spa_preprocess._transport_object")
def test_register_grid_square_update_add_nothing(
    mock_transport, murfey_db_session: Session
):
    """Test the updating of an existing grid square, but with nothing to update with"""
    # Create a grid square to update
    grid_square = GridSquare(
        id=1,
        name=101,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        x_location=0.1,
        y_location=0.2,
        x_stage_position=0.3,
        y_stage_position=0.4,
    )
    murfey_db_session.add(grid_square)
    murfey_db_session.commit()

    # Parameters to update with
    new_parameters = GridSquareParameters(tag="session_tag")

    # Run the registration
    flush_spa_preprocess.register_grid_square(
        ExampleVisit.murfey_session_id, 101, new_parameters, murfey_db_session
    )

    # Check this would have updated ispyb
    mock_transport.do_update_grid_square.assert_called_with(1, new_parameters)

    # Confirm the database was not updated
    grid_square_final_parameters = murfey_db_session.exec(select(GridSquare)).one()
    assert grid_square_final_parameters.x_location == 0.1
    assert grid_square_final_parameters.y_location == 0.2
    assert grid_square_final_parameters.x_stage_position == 0.3
    assert grid_square_final_parameters.y_stage_position == 0.4


@mock.patch("murfey.workflows.spa.flush_spa_preprocess._transport_object")
def test_register_grid_square_insert_with_ispyb(
    mock_transport, murfey_db_session: Session, tmp_path
):
    # Create a data collection group for lookups
    grid_square = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        atlas_id=90,
    )
    murfey_db_session.add(grid_square)
    murfey_db_session.commit()

    # Set the ispyb return
    mock_transport.do_insert_grid_square.return_value = {
        "return_value": 1,
        "success": True,
    }

    # Parameters to update with
    new_parameters = GridSquareParameters(
        tag="session_tag",
        x_location=1.1,
        y_location=1.2,
        x_stage_position=1.3,
        y_stage_position=1.4,
        readout_area_x=2048,
        readout_area_y=1024,
        thumbnail_size_x=512,
        thumbnail_size_y=256,
        pixel_size=1.02,
        image=f"{tmp_path}/image_path",
        angle=12.5,
    )

    # Run the registration
    flush_spa_preprocess.register_grid_square(
        ExampleVisit.murfey_session_id, 101, new_parameters, murfey_db_session
    )

    # Check this would have updated ispyb
    mock_transport.do_insert_grid_square.assert_called_with(90, 101, new_parameters)

    # Confirm the database entry was made
    grid_square_final_parameters = murfey_db_session.exec(select(GridSquare)).one()
    assert grid_square_final_parameters.id == 1
    assert grid_square_final_parameters.name == 101
    assert grid_square_final_parameters.session_id == ExampleVisit.murfey_session_id
    assert grid_square_final_parameters.tag == "session_tag"
    assert grid_square_final_parameters.x_location == 1.1
    assert grid_square_final_parameters.y_location == 1.2
    assert grid_square_final_parameters.x_stage_position == 1.3
    assert grid_square_final_parameters.y_stage_position == 1.4
    assert grid_square_final_parameters.readout_area_x == 2048
    assert grid_square_final_parameters.readout_area_y == 1024
    assert grid_square_final_parameters.thumbnail_size_x == 512
    assert grid_square_final_parameters.thumbnail_size_y == 256
    assert grid_square_final_parameters.pixel_size == 1.02
    assert grid_square_final_parameters.image == f"{tmp_path}/image_path"
