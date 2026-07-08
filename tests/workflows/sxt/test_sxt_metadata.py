import copy
from unittest import mock

from sqlmodel import Session, select

from murfey.util.db import (
    DataCollectionGroup,
    ImagingSite,
    SearchMap,
)
from murfey.util.models import SearchMapParameters
from murfey.workflows.sxt import sxt_metadata
from tests.conftest import ExampleVisit, get_or_create_db_entry


def set_up_db(murfey_db_session: Session):
    # Insert common elements needed in all tests
    dcg_entry: DataCollectionGroup = get_or_create_db_entry(
        murfey_db_session,
        DataCollectionGroup,
        lookup_kwargs={
            "id": 0,
            "session_id": ExampleVisit.murfey_session_id,
            "tag": "/path/to/tomogram_source",
            "atlas_id": 10,
        },
    )
    get_or_create_db_entry(
        murfey_db_session,
        ImagingSite,
        lookup_kwargs={
            "dcg_id": dcg_entry.id,
            "site_name": "site",
            "pos_x": 2,
            "pos_y": 3,
            "image_pixel_size": 0.5,
            "image_pixels_x": 400,
            "image_pixels_y": 500,
        },
    )
    return dcg_entry.id


@mock.patch("murfey.workflows.sxt.process_sxt_tilt_series._transport_object")
def test_register_new_sxt_roi(mock_transport, murfey_db_session: Session, tmp_path):
    set_up_db(murfey_db_session)

    roi_params = SearchMapParameters(
        tag="/path/to/tomogram_source",
        x_stage_position=10,
        y_stage_position=20,
        pixel_size=None,
        width=None,
        height=None,
        image="/path/to/image.jpg",
    )
    return_dict = sxt_metadata.register_sxt_roi(
        ExampleVisit.murfey_session_id, "roi_1", roi_params, murfey_db_session
    )
    assert return_dict.get("success")

    # Check the ispyb message
    mock_transport.do_insert_sxt_roi.assert_called_once_with(10, roi_params)

    # Check the database insert
    roi_entry = murfey_db_session.exec(select(SearchMap)).one()
    assert roi_entry.session_id == ExampleVisit.murfey_session_id
    assert roi_entry.name == "roi_1"
    assert roi_entry.tag == "/path/to/tomogram_source"
    assert roi_entry.x_stage_position == 10
    assert roi_entry.y_stage_position == 2
    assert not roi_entry.pixel_size
    assert not roi_entry.width
    assert not roi_entry.height
    assert roi_entry.image == "/path/to/image.jpg"


@mock.patch("murfey.workflows.sxt.process_sxt_tilt_series._transport_object")
def test_update_sxt_roi(mock_transport, murfey_db_session: Session, tmp_path):
    set_up_db(murfey_db_session)

    get_or_create_db_entry(
        murfey_db_session,
        SearchMap,
        lookup_kwargs={
            "session_id": ExampleVisit.murfey_session_id,
            "name": "roi_1",
            "tag": "/path/to/tomogram_source",
        },
    )

    roi_params = SearchMapParameters(
        id=2,
        tag="/path/to/tomogram_source",
        x_stage_position=10,
        y_stage_position=20,
        pixel_size=0.025,
        width=200,
        height=400,
        image="/path/to/image.jpg",
    )
    return_dict = sxt_metadata.register_sxt_roi(
        ExampleVisit.murfey_session_id,
        "roi_1",
        copy.deepcopy(roi_params),
        murfey_db_session,
    )
    assert return_dict.get("success")

    # Check the ispyb message
    mock_transport.do_update_sxt_roi.assert_any_call(2, roi_params)

    # Check the second update
    roi_params.x_location = 16 * (512 / 400) + 256
    roi_params.y_location = 256 - 34 * (512 / 500) + 256
    roi_params.width_on_atlas = 2 * 512 / 400
    roi_params.height_on_atlas = 4 * 512 / 500

    # Check the database insert
    roi_entry = murfey_db_session.exec(select(SearchMap)).one()
    assert roi_entry.session_id == ExampleVisit.murfey_session_id
    assert roi_entry.name == "roi_1"
    assert roi_entry.tag == "/path/to/tomogram_source"
    assert roi_entry.x_stage_position == 10
    assert roi_entry.y_stage_position == 2
    assert roi_entry.pixel_size == 0.025
    assert roi_entry.width == 200
    assert roi_entry.height == 400
    assert roi_entry.image == "/path/to/image.jpg"
    assert roi_entry.x_location == 16
    assert roi_entry.y_location == 34
