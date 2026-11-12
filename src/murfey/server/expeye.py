import json
import logging

import requests

from murfey.util.config import get_security_config
from murfey.util.models import FoilHoleParameters

EXPEYE_TOKEN = get_security_config().expeye_token
EXPEYE_URL = get_security_config().expeye_url

logger = logging.getLogger(__name__)


def get(url: str) -> requests.Response:
    return requests.get(
        f"{EXPEYE_URL}/{url}", headers={"Authorization": f"access_token {EXPEYE_TOKEN}"}
    )


def post(url: str, json: dict | list | None = None) -> requests.Response:
    return requests.post(
        f"{EXPEYE_URL}/{url}",
        json=json or {},
        headers={"Authorization": f"access_token {EXPEYE_TOKEN}"},
    )


def patch(url: str, json: dict | list | None = None) -> requests.Response:
    return requests.patch(
        f"{EXPEYE_URL}/{url}",
        json=json or {},
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
                "qualityIndicator": fhp.quality_indicator,
                "pixelSize": pixel_size,
            }
        )
    response = post(f"grid-squares/{grid_square_id}/foil-holes", json=foil_holes)
    try:
        result = response.json()
        return {fh["foilHoleLabel"]: fh["foilHoleId"] for fh in result}
    except json.JSONDecodeError:
        return None


def update_foil_holes(
    grid_square_id: int,
    scale_factor: float,
    foil_hole_ids: list[int],
    foil_hole_parameters: list[FoilHoleParameters],
) -> dict | None:
    foil_holes = []
    for fhid, fhp in zip(foil_hole_ids, foil_hole_parameters):
        foil_hole_dict: dict = {"foilHoleId": fhid}
        if fhp.image:
            foil_hole_dict["foilHoleImage"] = fhp.image
        if fhp.x_location:
            foil_hole_dict["pixelLocationX"] = int(fhp.x_location * scale_factor)
        if fhp.y_location:
            foil_hole_dict["pixelLocationY"] = int(fhp.y_location * scale_factor)
        if fhp.diameter is not None:
            foil_hole_dict["diameter"] = fhp.diameter * scale_factor
        if fhp.x_stage_position:
            foil_hole_dict["stageLocationX"] = fhp.x_stage_position
        if fhp.y_stage_position:
            foil_hole_dict["stageLocationY"] = fhp.y_stage_position
        if fhp.quality_indicator:
            foil_hole_dict["qualityIndicator"] = fhp.quality_indicator
        if (
            fhp.readout_area_x is not None
            and fhp.thumbnail_size_x is not None
            and fhp.pixel_size is not None
        ):
            foil_hole_dict["pixelSize"] = fhp.pixel_size * (
                fhp.readout_area_x / fhp.thumbnail_size_x
            )
        foil_holes.append(foil_hole_dict)
    response = patch(f"grid-squares/{grid_square_id}/foil-holes", json=foil_holes)
    try:
        result = response.json()
        return {fh["foilHoleLabel"]: fh["foilHoleId"] for fh in result}
    except json.JSONDecodeError:
        return None
