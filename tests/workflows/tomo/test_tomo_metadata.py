from unittest import mock

from sqlmodel import Session, select

from murfey.util.db import DataCollectionGroup, SearchMap, TiltSeries
from murfey.util.models import BatchPositionParameters, SearchMapParameters
from murfey.workflows.tomo import tomo_metadata
from tests.conftest import ExampleVisit


@mock.patch("murfey.workflows.tomo.tomo_metadata._transport_object")
def test_register_search_map_update_with_dimensions(
    mock_transport, murfey_db_session: Session
):
    """Test the updating of an existing search map, without enough to find location"""
    # Create a search map to update
    search_map = SearchMap(
        id=1,
        name="SearchMap_1",
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        x_stage_position=0.1,
        y_stage_position=0.2,
    )
    murfey_db_session.add(search_map)
    murfey_db_session.commit()

    # Make sure DCG is present with a pixel size
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        atlas_id=90,
        atlas_pixel_size=1e-5,
    )
    murfey_db_session.add(dcg)
    murfey_db_session.commit()

    # Parameters to update with
    new_parameters = SearchMapParameters(
        tag="session_tag",
        width=2000,
        height=4000,
    )

    # Run the registration
    tomo_metadata.register_search_map_in_database(
        ExampleVisit.murfey_session_id, "SearchMap_1", new_parameters, murfey_db_session
    )

    # Check this would have updated ispyb
    mock_transport.do_update_search_map.assert_called_with(1, new_parameters)

    # Confirm the database was updated
    sm_final_parameters = murfey_db_session.exec(select(SearchMap)).one()
    assert sm_final_parameters.width == new_parameters.width
    assert sm_final_parameters.height == new_parameters.height
    assert sm_final_parameters.x_stage_position == 0.1
    assert sm_final_parameters.y_stage_position == 0.2
    assert sm_final_parameters.x_location is None


@mock.patch("murfey.workflows.tomo.tomo_metadata._transport_object")
def test_register_search_map_update_with_all_parameters(
    mock_transport, murfey_db_session: Session
):
    """Test the updating of an existing search map with all required parameters"""
    # Create a search map to update
    search_map = SearchMap(
        id=1,
        name="SearchMap_1",
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        x_stage_position=0.1,
        y_stage_position=0.2,
        width=2000,
        height=4000,
    )
    murfey_db_session.add(search_map)
    murfey_db_session.commit()

    # Make sure DCG is present with a pixel size
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        atlas_id=90,
        atlas_pixel_size=1e-5,
    )
    murfey_db_session.add(dcg)
    murfey_db_session.commit()

    # Parameters to update with
    new_parameters = SearchMapParameters(
        tag="session_tag",
        x_stage_position=0.3,
        y_stage_position=0.4,
        pixel_size=1e-7,
        image="path/to/image",
        binning=1,
        reference_matrix={"m11": 1.01, "m12": 0.01, "m21": 0.02, "m22": 1.02},
        stage_correction={"m11": 0.99, "m12": -0.01, "m21": -0.02, "m22": 0.98},
        image_shift_correction={"m11": 1.03, "m12": 0.03, "m21": -0.03, "m22": 0.97},
    )

    # Run the registration
    tomo_metadata.register_search_map_in_database(
        ExampleVisit.murfey_session_id, "SearchMap_1", new_parameters, murfey_db_session
    )

    # Confirm the database was updated
    sm_final_parameters = murfey_db_session.exec(select(SearchMap)).one()
    assert sm_final_parameters.width == 2000
    assert sm_final_parameters.height == 4000
    assert sm_final_parameters.x_stage_position == 0.3
    assert sm_final_parameters.y_stage_position == 0.4
    assert sm_final_parameters.pixel_size == 1e-7
    assert sm_final_parameters.image == "path/to/image"
    assert sm_final_parameters.binning == 1
    assert sm_final_parameters.reference_matrix_m11 == 1.01
    assert sm_final_parameters.reference_matrix_m12 == 0.01
    assert sm_final_parameters.reference_matrix_m21 == 0.02
    assert sm_final_parameters.reference_matrix_m22 == 1.02
    assert sm_final_parameters.stage_correction_m11 == 0.99
    assert sm_final_parameters.stage_correction_m12 == -0.01
    assert sm_final_parameters.stage_correction_m21 == -0.02
    assert sm_final_parameters.stage_correction_m22 == 0.98
    assert sm_final_parameters.image_shift_correction_m11 == 1.03
    assert sm_final_parameters.image_shift_correction_m12 == 0.03
    assert sm_final_parameters.image_shift_correction_m21 == -0.03
    assert sm_final_parameters.image_shift_correction_m22 == 0.97

    # These two should have been updated, but what that update should be is messy
    assert sm_final_parameters.x_location is not None
    assert sm_final_parameters.y_location is not None

    # Check this would have updated ispyb
    mock_transport.do_update_search_map.assert_called_with(1, new_parameters)
    new_parameters.x_location = sm_final_parameters.x_location
    new_parameters.y_location = sm_final_parameters.y_location
    new_parameters.height_on_atlas = 40
    new_parameters.width_on_atlas = 20
    mock_transport.do_update_search_map.assert_called_with(1, new_parameters)


@mock.patch("murfey.workflows.tomo.tomo_metadata._transport_object")
def test_register_search_map_insert_with_ispyb(
    mock_transport, murfey_db_session: Session, tmp_path
):
    """Insert a new search map"""
    # Create a data collection group for lookups
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        atlas_id=90,
        atlas_pixel_size=1e-5,
    )
    murfey_db_session.add(dcg)
    murfey_db_session.commit()

    # Set the ispyb return
    mock_transport.do_insert_search_map.return_value = {
        "return_value": 1,
        "success": True,
    }

    # Parameters to update with
    new_parameters = SearchMapParameters(
        tag="session_tag",
        x_stage_position=1.3,
        y_stage_position=1.4,
        pixel_size=1.02,
    )

    # Run the registration
    tomo_metadata.register_search_map_in_database(
        ExampleVisit.murfey_session_id, "SearchMap_1", new_parameters, murfey_db_session
    )

    # Check this would have updated ispyb
    mock_transport.do_insert_search_map.assert_called_with(90, new_parameters)

    # Confirm the database entry was made
    sm_final_parameters = murfey_db_session.exec(select(SearchMap)).one()
    assert sm_final_parameters.id == 1
    assert sm_final_parameters.name == "SearchMap_1"
    assert sm_final_parameters.session_id == ExampleVisit.murfey_session_id
    assert sm_final_parameters.tag == "session_tag"
    assert sm_final_parameters.x_stage_position == 1.3
    assert sm_final_parameters.y_stage_position == 1.4
    assert sm_final_parameters.pixel_size == 1.02
    assert sm_final_parameters.x_location is None


def test_register_batch_position_update(murfey_db_session: Session):
    """Test the updating of an existing tilt series with batch positions"""
    # Make sure search map is present
    search_map = SearchMap(
        id=1,
        name="SearchMap_1",
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        x_stage_position=1,
        y_stage_position=2,
        pixel_size=0.01,
        reference_matrix_m11=1,
        reference_matrix_m12=0,
        reference_matrix_m21=0,
        reference_matrix_m22=1,
        stage_correction_m11=1,
        stage_correction_m12=0,
        stage_correction_m21=0,
        stage_correction_m22=1,
        image_shift_correction_m11=1,
        image_shift_correction_m12=0,
        image_shift_correction_m21=0,
        image_shift_correction_m22=1,
        height=4000,
        width=2000,
    )
    murfey_db_session.add(search_map)
    murfey_db_session.commit()

    # Create a tilt series to update
    tilt_series = TiltSeries(
        tag="Position_1",
        rsync_source="session_tag",
        session_id=ExampleVisit.murfey_session_id,
        search_map_id=1,
    )
    murfey_db_session.add(tilt_series)
    murfey_db_session.commit()

    # Parameters to update with
    new_parameters = BatchPositionParameters(
        tag="session_tag",
        x_stage_position=0.1,
        y_stage_position=0.2,
        x_beamshift=0.3,
        y_beamshift=0.4,
        search_map_name="SearchMap_1",
    )

    # Run the registration
    tomo_metadata.register_batch_position_in_database(
        ExampleVisit.murfey_session_id, "Position_1", new_parameters, murfey_db_session
    )

    # These two should have been updated, values are known as used identity matrices
    bp_final_parameters = murfey_db_session.exec(select(TiltSeries)).one()
    assert bp_final_parameters.x_location == 880
    assert bp_final_parameters.y_location == 1780


def test_register_batch_position_update_skip(murfey_db_session: Session):
    """Test the updating of an existing batch position, skipped as already done"""
    # Make sure search map is present
    search_map = SearchMap(
        id=1,
        name="SearchMap_1",
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        x_stage_position=1,
        y_stage_position=2,
        pixel_size=0.01,
        reference_matrix_m11=1,
        reference_matrix_m12=0,
        reference_matrix_m21=0,
        reference_matrix_m22=1,
        stage_correction_m11=1,
        stage_correction_m12=0,
        stage_correction_m21=0,
        stage_correction_m22=1,
        image_shift_correction_m11=1,
        image_shift_correction_m12=0,
        image_shift_correction_m21=0,
        image_shift_correction_m22=1,
        height=4000,
        width=2000,
    )
    murfey_db_session.add(search_map)
    murfey_db_session.commit()

    # Create a tilt series to update
    tilt_series = TiltSeries(
        tag="Position_1",
        rsync_source="session_tag",
        session_id=ExampleVisit.murfey_session_id,
        search_map_id=1,
        x_location=100,
        y_location=200,
    )
    murfey_db_session.add(tilt_series)
    murfey_db_session.commit()

    # Parameters to update with
    new_parameters = BatchPositionParameters(
        tag="session_tag",
        x_stage_position=0.1,
        y_stage_position=0.2,
        x_beamshift=0.3,
        y_beamshift=0.4,
        search_map_name="SearchMap_1",
    )

    # Run the registration
    tomo_metadata.register_batch_position_in_database(
        ExampleVisit.murfey_session_id, "Position_1", new_parameters, murfey_db_session
    )

    # These two should have been updated, values are known as used identity matrices
    bp_final_parameters = murfey_db_session.exec(select(TiltSeries)).one()
    assert bp_final_parameters.x_location == 100
    assert bp_final_parameters.y_location == 200


def test_register_batch_position_new(murfey_db_session: Session):
    """Test the registration of a new tilt series with batch positions"""
    # Make sure search map is present
    search_map = SearchMap(
        id=1,
        name="SearchMap_1",
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        x_stage_position=1,
        y_stage_position=2,
        pixel_size=0.01,
        reference_matrix_m11=1,
        reference_matrix_m12=0,
        reference_matrix_m21=0,
        reference_matrix_m22=1,
        stage_correction_m11=1,
        stage_correction_m12=0,
        stage_correction_m21=0,
        stage_correction_m22=1,
        image_shift_correction_m11=1,
        image_shift_correction_m12=0,
        image_shift_correction_m21=0,
        image_shift_correction_m22=1,
        height=4000,
        width=2000,
    )
    murfey_db_session.add(search_map)
    murfey_db_session.commit()

    # Parameters to update with
    new_parameters = BatchPositionParameters(
        tag="session_tag",
        x_stage_position=0.1,
        y_stage_position=0.2,
        x_beamshift=0.3,
        y_beamshift=0.4,
        search_map_name="SearchMap_1",
    )

    # Run the registration
    tomo_metadata.register_batch_position_in_database(
        ExampleVisit.murfey_session_id, "Position_1", new_parameters, murfey_db_session
    )

    # These two should have been updated, values are known as used identity matrices
    bp_final_parameters = murfey_db_session.exec(select(TiltSeries)).one()
    assert bp_final_parameters.x_location == 880
    assert bp_final_parameters.y_location == 1780
