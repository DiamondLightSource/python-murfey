import logging
from pathlib import Path
from typing import Dict, NamedTuple, Optional, Tuple, Union

import xmltodict

logger = logging.getLogger("murfey.util.spa_metadata")


class FoilHoleInfo(NamedTuple):
    id: int
    grid_square_id: int
    x_location: Optional[float] = None
    y_location: Optional[float] = None
    x_stage_position: Optional[float] = None
    y_stage_position: Optional[float] = None
    readout_area_x: Optional[int] = None
    readout_area_y: Optional[int] = None
    thumbnail_size_x: Optional[int] = None
    thumbnail_size_y: Optional[int] = None
    pixel_size: Optional[float] = None
    image: str = ""
    diameter: Optional[float] = None


class GridSquareInfo(NamedTuple):
    id: int
    x_location: Optional[float] = None
    y_location: Optional[float] = None
    x_stage_position: Optional[float] = None
    y_stage_position: Optional[float] = None
    readout_area_x: Optional[int] = None
    readout_area_y: Optional[int] = None
    thumbnail_size_x: Optional[int] = None
    thumbnail_size_y: Optional[int] = None
    pixel_size: Optional[float] = None
    image: str = ""
    tag: str = ""


def grid_square_from_file(f: Path) -> int:
    for p in f.parts:
        if p.startswith("GridSquare"):
            return int(p.split("_")[1])
    raise ValueError(f"Grid square ID could not be determined from path {f}")


def foil_hole_from_file(f: Path) -> int:
    return int(f.name.split("_")[1])


def get_grid_square_atlas_positions(xml_path: Path, grid_square: str = "") -> Dict[
    str,
    Tuple[
        Optional[int],
        Optional[int],
        Optional[float],
        Optional[float],
        Optional[int],
        Optional[int],
        Optional[float],
    ],
]:
    with open(
        xml_path,
        "r",
    ) as dm:
        atlas_data = xmltodict.parse(dm.read())
    tile_info = atlas_data["AtlasSessionXml"]["Atlas"]["TilesEfficient"]["_items"][
        "TileXml"
    ]
    gs_pix_positions: Dict[
        str,
        Tuple[
            Optional[int],
            Optional[int],
            Optional[float],
            Optional[float],
            Optional[int],
            Optional[int],
            Optional[float],
        ],
    ] = {}
    for ti in tile_info:
        try:
            nodes = ti["Nodes"]["KeyValuePairs"]
        except KeyError:
            continue
        required_key = ""
        for key in nodes.keys():
            if key.startswith("KeyValuePairOfintNodeXml"):
                required_key = key
                break
        if not required_key:
            continue
        for gs in nodes[required_key]:
            if not isinstance(gs, dict):
                continue
            if not grid_square or gs["key"] == grid_square:
                gs_pix_positions[gs["key"]] = (
                    int(float(gs["value"]["b:PositionOnTheAtlas"]["c:Center"]["d:x"])),
                    int(float(gs["value"]["b:PositionOnTheAtlas"]["c:Center"]["d:y"])),
                    float(gs["value"]["b:PositionOnTheAtlas"]["c:Physical"]["d:x"])
                    * 1e9,
                    float(gs["value"]["b:PositionOnTheAtlas"]["c:Physical"]["d:y"])
                    * 1e9,
                    int(
                        float(gs["value"]["b:PositionOnTheAtlas"]["c:Size"]["d:width"])
                    ),
                    int(
                        float(gs["value"]["b:PositionOnTheAtlas"]["c:Size"]["d:height"])
                    ),
                    float(gs["value"]["b:PositionOnTheAtlas"]["c:Rotation"]),
                )
                if grid_square:
                    break
    return gs_pix_positions


def grid_square_data(xml_path: Path, grid_square: int) -> GridSquareInfo:
    image_paths = list(
        (xml_path.parent.parent).glob(
            f"Images-Disc*/GridSquare_{grid_square}/GridSquare_*.jpg"
        )
    )
    logger.info(
        f"{len(image_paths)} images found when searching {str(xml_path.parent.parent)}"
    )
    if image_paths:
        image_paths.sort(key=lambda x: x.stat().st_ctime)
        image_path = image_paths[-1]
        with open(Path(image_path).with_suffix(".xml")) as gs_xml:
            gs_xml_data = xmltodict.parse(gs_xml.read())
        readout_area = gs_xml_data["MicroscopeImage"]["microscopeData"]["acquisition"][
            "camera"
        ]["ReadoutArea"]
        pixel_size = gs_xml_data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
            "numericValue"
        ]
        full_size = (int(readout_area["a:width"]), int(readout_area["a:height"]))
        return GridSquareInfo(
            id=grid_square,
            readout_area_x=full_size[0] if image_path else None,
            readout_area_y=full_size[1] if image_path else None,
            thumbnail_size_x=int((512 / max(full_size)) * full_size[0]),
            thumbnail_size_y=int((512 / max(full_size)) * full_size[1]),
            pixel_size=float(pixel_size) if image_path else None,
            image=str(image_path),
        )
    return GridSquareInfo(id=grid_square)


def foil_hole_data(xml_path: Path, foil_hole: int, grid_square: int) -> FoilHoleInfo:
    required_key = ""
    if xml_path.is_file():
        with open(xml_path, "r") as xml:
            for_parsing = xml.read()
            data = xmltodict.parse(for_parsing)
        data = data["GridSquareXml"]
        if "TargetLocationsEfficient" in data["TargetLocations"].keys():
            # Grids with regular foil holes
            serialization_array = data["TargetLocations"]["TargetLocationsEfficient"][
                "a:m_serializationArray"
            ]
        elif "TargetLocations" in data["TargetLocations"].keys():
            # Lacey grids
            serialization_array = data["TargetLocations"]["TargetLocations"][
                "a:m_serializationArray"
            ]
        else:
            logger.warning(f"Target locations not found for {str(xml_path)}")
            return FoilHoleInfo(id=foil_hole, grid_square_id=grid_square)

        if len(serialization_array.keys()) == 0:
            return FoilHoleInfo(id=foil_hole, grid_square_id=grid_square)
        for key in serialization_array.keys():
            if key.startswith("b:KeyValuePairOfintTargetLocation"):
                required_key = key
                break
    image_paths = list(
        (xml_path.parent.parent).glob(
            f"Images-Disc*/GridSquare_{grid_square}/FoilHoles/FoilHole_{foil_hole}_*.jpg"
        )
    )
    image_paths.sort(key=lambda x: x.stat().st_ctime)
    image_path: Union[Path, str] = image_paths[-1] if image_paths else ""
    if image_path:
        with open(Path(image_path).with_suffix(".xml")) as fh_xml:
            fh_xml_data = xmltodict.parse(fh_xml.read())
        readout_area = fh_xml_data["MicroscopeImage"]["microscopeData"]["acquisition"][
            "camera"
        ]["ReadoutArea"]
        pixel_size = fh_xml_data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
            "numericValue"
        ]
        full_size = (int(readout_area["a:width"]), int(readout_area["a:height"]))
    if required_key:
        for fh_block in serialization_array[required_key]:
            pix = fh_block["b:value"]["PixelCenter"]
            stage = fh_block["b:value"]["StagePosition"]
            diameter = fh_block["b:value"]["PixelWidthHeight"]["c:width"]
            if int(fh_block["b:key"]) == foil_hole:
                return FoilHoleInfo(
                    id=foil_hole,
                    grid_square_id=grid_square,
                    x_location=float(pix["c:x"]),
                    y_location=float(pix["c:y"]),
                    x_stage_position=float(stage["c:X"]),
                    y_stage_position=float(stage["c:Y"]),
                    readout_area_x=full_size[0] if image_path else None,
                    readout_area_y=full_size[1] if image_path else None,
                    thumbnail_size_x=None,
                    thumbnail_size_y=None,
                    pixel_size=float(pixel_size) if image_path else None,
                    image=str(image_path),
                    diameter=diameter,
                )
    elif image_path:
        return FoilHoleInfo(
            id=foil_hole,
            grid_square_id=grid_square,
            readout_area_x=full_size[0] if image_path else None,
            readout_area_y=full_size[1] if image_path else None,
            thumbnail_size_x=None,
            thumbnail_size_y=None,
            pixel_size=float(pixel_size) if image_path else None,
            image=str(image_path),
        )
    logger.warning(
        f"Foil hole positions could not be determined from metadata file {xml_path} for foil hole {foil_hole}"
    )
    return FoilHoleInfo(id=foil_hole, grid_square_id=grid_square)
