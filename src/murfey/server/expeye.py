import json

import requests

from murfey.util.config import get_security_config
from murfey.util.models import FoilHoleParameters

EXPEYE_TOKEN = get_security_config().expeye_token
EXPEYE_URL = get_security_config().expeye_url


def get(url: str) -> requests.Response:
    return requests.get(
        f"{EXPEYE_URL}/{url}", headers={"Authorization": f"access_token {EXPEYE_TOKEN}"}
    )


def post(url: str, json: dict | list | None = None) -> requests.Response:
    json = json or {}
    return requests.post(
        f"{EXPEYE_URL}/{url}",
        json=json,
        headers={"Authorization": f"access_token {EXPEYE_TOKEN}"},
    )


def insert_foil_holes(
    grid_square_id: int,
    scale_factor: float | None,
    foil_hole_parameters: list[FoilHoleParameters],
) -> dict | None:
    foil_holes = []
    for fhp in foil_hole_parameters:
        pixel_size = fhp.pixel_size
        diameter = fhp.diameter
        x_location = fhp.x_location
        y_location = fhp.y_location
        if (
            fhp.thumbnail_size_x is not None
            and fhp.readout_area_x is not None
            and pixel_size is not None
        ):
            pixel_size *= fhp.readout_area_x / fhp.thumbnail_size_x
        if scale_factor:
            diameter = int(fhp.diameter * scale_factor) if fhp.diameter else None
            x_location = int(fhp.x_location * scale_factor) if fhp.x_location else None
            y_location = int(fhp.y_location * scale_factor) if fhp.y_location else None
        foil_holes.append(
            {
                "foilHoleLabel": fhp.name,
                "foilHoleImage": fhp.image,
                "pixelLocationX": x_location,
                "pixelLocationY": y_location,
                "diameter": diameter,
                "stageLocationX": fhp.x_stage_position,
                "stageLocationY": fhp.y_stage_position,
                "qualityIndicator": 0,
                "pixelSize": pixel_size,
            }
        )
    response = post(f"grid-squares/{grid_square_id}/foil-holes", json=foil_holes)
    try:
        return response.json()
    except json.JSONDecodeError:
        return None
