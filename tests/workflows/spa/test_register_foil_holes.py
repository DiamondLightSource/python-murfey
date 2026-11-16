from unittest import mock

from sqlmodel import Session, select

from murfey.util.db import DataCollectionGroup, FoilHole, GridSquare
from murfey.util.models import FoilHoleParameters
from murfey.workflows.spa import register_foil_holes
from tests.conftest import ExampleVisit


@mock.patch("murfey.server._transport_object")
def test_register_foil_hole_add_and_update(mock_transport, murfey_db_session: Session):
    """Test inserting and updating foil holes"""
    mock_transport.do_update_foil_holes.return_value = {
        "success": True,
        "return_value": {1001: 1, 2002: 2},
    }
    mock_transport.do_insert_foil_holes.return_value = {
        "success": True,
        "return_value": {3003: 3, 4004: 4},
    }

    # Create a grid square to update
    dcg = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        atlas_id=90,
        sample=2,
    )
    murfey_db_session.add(dcg)
    grid_square = GridSquare(
        id=1,
        name=101,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        thumbnail_size_x=10,
        readout_area_x=5,
    )
    murfey_db_session.add(grid_square)
    foil_hole_1 = FoilHole(
        id=1,
        name=1001,
        grid_square_id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
    )
    murfey_db_session.add(foil_hole_1)
    foil_hole_2 = FoilHole(
        id=2,
        name=2002,
        grid_square_id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
    )
    murfey_db_session.add(foil_hole_2)
    murfey_db_session.commit()

    foil_hole_group = {
        i * 1000 + i: FoilHoleParameters(
            tag="session_tag",
            name=i * 1000 + i,
            x_location=0.5,
            y_location=-0.5,
            x_stage_position=1.5,
            y_stage_position=-1.5,
            readout_area_x=5760,
            readout_area_y=4092,
            thumbnail_size_x=1024,
            thumbnail_size_y=512,
            pixel_size=1e-6,
            image="/path/to/image",
            diameter=1.5,
        )
        for i in range(1, 5)
    }

    hole_ids = register_foil_holes.register_foil_holes(
        session_id=ExampleVisit.murfey_session_id,
        gs_name=101,
        foil_hole_group=foil_hole_group,
        murfey_db=murfey_db_session,
    )

    assert len(hole_ids) == 4

    # Check this would have updated ispyb
    mock_transport.do_update_foil_holes.assert_called_once_with(
        1, 2, [1, 2], [foil_hole_group[1001], foil_hole_group[2002]]
    )
    mock_transport.do_insert_foil_holes.assert_called_once_with(
        1, 2, [foil_hole_group[3003], foil_hole_group[4004]]
    )

    # Confirm the database was updated
    foil_hole_final_parameters = murfey_db_session.exec(select(FoilHole)).all()
    assert len(foil_hole_final_parameters) == 4
    for i, final_foil_hole in enumerate(foil_hole_final_parameters):
        assert final_foil_hole.name == (i + 1) * 1000 + i + 1
        assert final_foil_hole.session_id == ExampleVisit.murfey_session_id
        assert final_foil_hole.grid_square_id == 1
        assert final_foil_hole.x_location == 0.5
        assert final_foil_hole.y_location == -0.5
        assert final_foil_hole.x_stage_position == 1.5
        assert final_foil_hole.y_stage_position == -1.5
        assert final_foil_hole.readout_area_x == 5760
        assert final_foil_hole.readout_area_y == 4092
        assert final_foil_hole.thumbnail_size_x == 1024
        assert final_foil_hole.thumbnail_size_y == 512
        assert final_foil_hole.pixel_size == 1e-6
        assert final_foil_hole.image == "/path/to/image"
        assert final_foil_hole.smartem_uuid is None
