from unittest import mock

from murfey.server import expeye
from murfey.util.models import FoilHoleParameters


@mock.patch("murfey.server.expeye.requests.post")
def test_insert_foil_holes(mock_post):
    mock_post().json.return_value = [
        {"foilHoleLabel": 1001, "foilHoleId": 1},
        {"foilHoleLabel": 2002, "foilHoleId": 2},
    ]
    expeye.EXPEYE_TOKEN = "expeye"
    expeye.EXPEYE_URL = "http://expeye:1000"

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
            quality_indicator=0.5,
            diameter=1.5,
        )
        for i in range(1, 3)
    }

    # Check this would have updated ispyb
    expeye_return = expeye.insert_foil_holes(
        1, 2, [foil_hole_group[1001], foil_hole_group[2002]]
    )
    assert expeye_return[1001] == 1
    assert expeye_return[2002] == 2

    mock_post.assert_any_call(
        "http://expeye:1000/grid-squares/1/foil-holes",
        json=[
            {
                "foilHoleLabel": fh_name,
                "foilHoleImage": "/path/to/image",
                "pixelLocationX": 1,
                "pixelLocationY": -1,
                "diameter": 3,
                "stageLocationX": 1.5,
                "stageLocationY": -1.5,
                "qualityIndicator": 0.5,
                "pixelSize": 1e-6 * 5760 / 1024,
            }
            for fh_name in [1001, 2002]
        ],
        headers={"Authorization": "access_token expeye"},
    )
    expeye.EXPEYE_TOKEN = ""


@mock.patch("murfey.server.expeye.requests.patch")
def test_update_foil_holes(mock_patch):
    mock_patch().json.return_value = [
        {"foilHoleLabel": 1001, "foilHoleId": 1},
        {"foilHoleLabel": 2002, "foilHoleId": 2},
    ]
    expeye.EXPEYE_TOKEN = "expeye"
    expeye.EXPEYE_URL = "http://expeye:1000"

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
            quality_indicator=0.5,
            diameter=1.5,
        )
        for i in range(1, 3)
    }

    # Check this would have updated ispyb
    expeye_return = expeye.update_foil_holes(
        1, 2, [3, 4], [foil_hole_group[1001], foil_hole_group[2002]]
    )
    assert expeye_return[1001] == 1
    assert expeye_return[2002] == 2

    mock_patch.assert_any_call(
        "http://expeye:1000/grid-squares/1/foil-holes",
        json=[
            {
                "foilHoleId": fhid,
                "foilHoleImage": "/path/to/image",
                "pixelLocationX": 1,
                "pixelLocationY": -1,
                "diameter": 3,
                "stageLocationX": 1.5,
                "stageLocationY": -1.5,
                "qualityIndicator": 0.5,
                "pixelSize": 1e-6 * 5760 / 1024,
            }
            for fhid, fh_name in zip([3, 4], [1001, 2002])
        ],
        headers={"Authorization": "access_token expeye"},
    )
    expeye.EXPEYE_TOKEN = ""
