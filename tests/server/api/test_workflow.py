from unittest import mock

from sqlmodel import Session, select

from murfey.server.api.workflow import DCGroupParameters, register_dc_group
from murfey.util.db import DataCollectionGroup, SearchMap
from tests.conftest import ExampleVisit


@mock.patch("murfey.server.api.workflow._transport_object")
def test_register_dc_group_new_dcg(mock_transport, murfey_db_session: Session):
    """Test the request for a completely new data collection group"""
    mock_transport.feedback_queue = "mock_feedback_queue"

    # Request new dcg registration
    dcg_params = DCGroupParameters(
        experiment_type_id=44,
        tag="atlas_tag",
        atlas="/path/to/Atlas_1.jpg",
        sample=10,
        atlas_pixel_size=1e-5,
    )
    register_dc_group(
        visit_name="cm12345-6",
        session_id=ExampleVisit.murfey_session_id,
        dcg_params=dcg_params,
        db=murfey_db_session,
    )

    # Check request for registering dcg in ispyb and murfey
    mock_transport.send.assert_called_once_with(
        "mock_feedback_queue",
        {
            "register": "data_collection_group",
            "start_time": mock.ANY,
            "experiment_type_id": 44,
            "tag": "atlas_tag",
            "session_id": ExampleVisit.murfey_session_id,
            "atlas": "/path/to/Atlas_1.jpg",
            "sample": 10,
            "atlas_pixel_size": 1e-5,
            "microscope": "",
            "proposal_code": ExampleVisit.proposal_code,
            "proposal_number": str(ExampleVisit.proposal_number),
            "visit_number": str(ExampleVisit.visit_number),
        },
    )


@mock.patch("murfey.server.api.workflow._transport_object")
def test_register_dc_group_atlas_to_processing(
    mock_transport, murfey_db_session: Session
):
    """
    Test the request to update an existing data collection group
    from atlas type 44 to a processing type with a different tag
    """
    mock_transport.feedback_queue = "mock_feedback_queue"

    # Make sure dcg is present
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="atlas_tag",
        atlas_id=90,
        atlas_pixel_size=1e-5,
        sample=10,
        atlas="/path/to/Atlas_1.jpg",
    )
    murfey_db_session.add(dcg)
    murfey_db_session.commit()

    # Request new dcg registration with processing experiment type and tag
    dcg_params = DCGroupParameters(
        experiment_type_id=36,
        tag="processing_tag",
        atlas="/path/to/Atlas_1.jpg",
        sample=10,
        atlas_pixel_size=1e-5,
    )
    register_dc_group(
        visit_name="cm12345-6",
        session_id=ExampleVisit.murfey_session_id,
        dcg_params=dcg_params,
        db=murfey_db_session,
    )

    # Check request to ispyb for updating the experiment type
    mock_transport.send.assert_called_once_with(
        "mock_feedback_queue",
        {
            "register": "experiment_type_update",
            "experiment_type_id": 36,
            "dcgid": 1,
        },
    )

    # Check that the tag of the data collection group was updated
    new_dcg = murfey_db_session.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.id == 1)
    ).one()
    assert new_dcg.tag == "processing_tag"


@mock.patch("murfey.server.api.workflow._transport_object")
def test_register_dc_group_processing_to_atlas(
    mock_transport, murfey_db_session: Session
):
    """
    Test the request to update an existing data collection group
    of processing type with a new atlas type 44, which should leave the tag unchanged
    """
    mock_transport.feedback_queue = "mock_feedback_queue"

    # Make sure dcg is present
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="processing_tag",
        atlas_id=90,
        atlas_pixel_size=1e-5,
        sample=10,
        atlas="/path/to/Atlas_1.jpg",
    )
    murfey_db_session.add(dcg)
    murfey_db_session.commit()

    # Request new dcg registration with atlas experiment type and tag
    dcg_params = DCGroupParameters(
        experiment_type_id=44,
        tag="atlas_tag",
        atlas="/path/to/Atlas_2.jpg",
        sample=10,
        atlas_pixel_size=1e-4,
    )
    register_dc_group(
        visit_name="cm12345-6",
        session_id=ExampleVisit.murfey_session_id,
        dcg_params=dcg_params,
        db=murfey_db_session,
    )

    # Check request to ispyb for updating the experiment type
    mock_transport.send.assert_called_once_with(
        "mock_feedback_queue",
        {
            "register": "atlas_update",
            "atlas_id": 90,
            "atlas": "/path/to/Atlas_2.jpg",
            "sample": 10,
            "atlas_pixel_size": 1e-4,
            "dcgid": 1,
            "session_id": ExampleVisit.murfey_session_id,
        },
    )

    # Check the data collection group atlas was updated
    new_dcg = murfey_db_session.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.id == 1)
    ).one()
    assert new_dcg.atlas == "/path/to/Atlas_2.jpg"
    assert new_dcg.atlas_pixel_size == 1e-4
    # Check the tag of the data collection group was not updated
    assert new_dcg.tag != "atlas_tag"


@mock.patch("murfey.server.api.workflow._transport_object")
def test_register_dc_group_new_atlas(mock_transport, murfey_db_session: Session):
    """
    Test the request to update an existing data collection group
    by adding an atlas, using the same tag
    """
    mock_transport.feedback_queue = "mock_feedback_queue"
    mock_transport.do_insert_atlas.return_value = {"return_value": 5}

    # Make sure dcg is present without an atlas id
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="processing_tag",
    )
    murfey_db_session.add(dcg)
    murfey_db_session.commit()

    # Request new dcg registration with atlas and exisiting tag
    dcg_params = DCGroupParameters(
        experiment_type_id=36,
        tag="processing_tag",
        atlas="/path/to/Atlas_2.jpg",
        sample=10,
        atlas_pixel_size=1e-4,
    )
    register_dc_group(
        visit_name="cm12345-6",
        session_id=ExampleVisit.murfey_session_id,
        dcg_params=dcg_params,
        db=murfey_db_session,
    )

    # Check no sends are made by the transport object
    mock_transport.send.assert_not_called()

    # Check the call to insert the atlas into ispyb
    atlas_args = mock_transport.do_insert_atlas.call_args_list
    assert len(atlas_args) == 1
    assert atlas_args[0][0][0].dataCollectionGroupId == 1
    assert atlas_args[0][0][0].atlasImage == "/path/to/Atlas_2.jpg"
    assert atlas_args[0][0][0].pixelSize == 1e-4
    assert atlas_args[0][0][0].cassetteSlot == 10

    # Check the data collection group atlas was updated
    new_dcg = murfey_db_session.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.id == 1)
    ).one()
    assert new_dcg.atlas == "/path/to/Atlas_2.jpg"
    assert new_dcg.sample == 10
    assert new_dcg.atlas_pixel_size == 1e-4
    assert new_dcg.tag == "atlas_tag"
    assert new_dcg.atlas_id == 5


@mock.patch("murfey.server.api.workflow._transport_object")
@mock.patch("murfey.server.api.workflow.register_search_map_in_database")
def test_register_dc_group_new_atlas_with_searchmaps(
    mock_register_search_map, mock_transport, murfey_db_session: Session
):
    """
    Test the request to update an existing data collection group
    by adding an atlas, using the same tag, and also update search maps
    """
    mock_transport.feedback_queue = "mock_feedback_queue"

    # Make sure dcg is present with an atlas id
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="processing_tag",
        atlas_id=90,
        atlas_pixel_size=1e-5,
        sample=10,
        atlas="/path/to/Atlas_1.jpg",
    )
    murfey_db_session.add(dcg)
    murfey_db_session.commit()

    # Add some search maps with the dcg tag and one with a different tag
    sm1 = SearchMap(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="processing_tag",
        name="searchmap1",
    )
    sm2 = SearchMap(
        id=2,
        session_id=ExampleVisit.murfey_session_id,
        tag="processing_tag",
        name="searchmap2",
    )
    sm3 = SearchMap(
        id=3,
        session_id=ExampleVisit.murfey_session_id,
        tag="different_tag",
        name="searchmap3",
    )
    murfey_db_session.add(sm1)
    murfey_db_session.add(sm2)
    murfey_db_session.add(sm3)
    murfey_db_session.commit()

    # Request new dcg registration with new atlas tag and sample
    dcg_params = DCGroupParameters(
        experiment_type_id=37,
        tag="processing_tag",
        atlas="/path/to/Atlas_2.jpg",
        sample=12,
        atlas_pixel_size=1e-4,
    )
    register_dc_group(
        visit_name="cm12345-6",
        session_id=ExampleVisit.murfey_session_id,
        dcg_params=dcg_params,
        db=murfey_db_session,
    )

    # Check request to ispyb for updating the experiment type
    mock_transport.send.assert_called_once_with(
        "mock_feedback_queue",
        {
            "register": "atlas_update",
            "atlas_id": 90,
            "atlas": "/path/to/Atlas_2.jpg",
            "sample": 12,
            "atlas_pixel_size": 1e-4,
            "dcgid": 1,
            "session_id": ExampleVisit.murfey_session_id,
        },
    )

    # Check the data collection group atlas was updated
    new_dcg = murfey_db_session.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.id == dcg.id)
    ).one()
    assert new_dcg.atlas == "/path/to/Atlas_2.jpg"
    assert new_dcg.sample == 12
    assert new_dcg.atlas_pixel_size == 1e-4
    assert new_dcg.tag == "processing_tag"
    assert new_dcg.atlas_id == 90

    # Check the search map update calls
    assert mock_register_search_map.call_count == 2
    mock_register_search_map.assert_any_call(
        ExampleVisit.murfey_session_id,
        "searchmap1",
        mock.ANY,
        murfey_db_session,
        close_db=False,
    )
    mock_register_search_map.assert_any_call(
        ExampleVisit.murfey_session_id,
        "searchmap2",
        mock.ANY,
        murfey_db_session,
        close_db=False,
    )
