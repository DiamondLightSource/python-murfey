from __future__ import annotations

import logging
from datetime import datetime
from itertools import count
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, OrderedDict, Tuple

import requests
import xmltodict

from murfey.client.context import Context, ProcessingParameter
from murfey.client.instance_environment import (
    MovieTracker,
    MurfeyID,
    MurfeyInstanceEnvironment,
)
from murfey.util import (
    authorised_requests,
    capture_get,
    capture_post,
    get_machine_config,
)

logger = logging.getLogger("murfey.client.contexts.spa")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


class FoilHole(NamedTuple):
    session_id: int
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


class GridSquare(NamedTuple):
    session_id: int
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


def _get_grid_square_atlas_positions(
    xml_path: Path, grid_square: str = ""
) -> Dict[str, Tuple[Optional[int], Optional[int], Optional[float], Optional[float]]]:
    with open(
        xml_path,
        "r",
    ) as dm:
        atlas_data = xmltodict.parse(dm.read())
    tile_info = atlas_data["AtlasSessionXml"]["Atlas"]["TilesEfficient"]["_items"][
        "TileXml"
    ]
    gs_pix_positions: Dict[
        str, Tuple[Optional[int], Optional[int], Optional[float], Optional[float]]
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
                )
                if grid_square:
                    break
    return gs_pix_positions


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
):
    machine_config = get_machine_config(
        str(environment.url.geturl()), demo=environment.demo
    )
    if environment.visit in environment.default_destinations[source]:
        return (
            Path(machine_config.get("rsync_basepath", ""))
            / Path(environment.default_destinations[source])
            / file_path.relative_to(source)
        )
    return (
        Path(machine_config.get("rsync_basepath", ""))
        / Path(environment.default_destinations[source])
        / environment.visit
        / file_path.relative_to(source)
    )


def _grid_square_from_file(f: Path) -> int:
    for p in f.parts:
        if p.startswith("GridSquare"):
            return int(p.split("_")[1])
    raise ValueError(f"Grid square ID could not be determined from path {f}")


def _foil_hole_from_file(f: Path) -> int:
    return int(f.name.split("_")[1])


def _grid_square_metadata_file(
    f: Path, data_directories: Dict[Path, str], visit: str, grid_square: int
) -> Path:
    for dd in data_directories.keys():
        if str(f).startswith(str(dd)):
            base_dir = dd
            mid_dir = f.relative_to(dd).parent
            break
    else:
        raise ValueError(f"Could not determine grid square metadata path for {f}")
    return (
        base_dir
        / visit
        / mid_dir.parent.parent.parent
        / "Metadata"
        / f"GridSquare_{grid_square}.dm"
    )


def _grid_square_data(xml_path: Path, grid_square: int, session_id: int) -> GridSquare:
    image_paths = list(
        (xml_path.parent.parent).glob(
            f"Images-Disc*/GridSquare_{grid_square}/GridSquare_*.jpg"
        )
    )
    if image_paths:
        image_paths.sort(key=lambda x: x.stat().st_ctime)
        image_path = image_paths[0]
        with open(Path(image_path).with_suffix(".xml")) as gs_xml:
            gs_xml_data = xmltodict.parse(gs_xml.read())
        readout_area = gs_xml_data["MicroscopeImage"]["microscopeData"]["acquisition"][
            "camera"
        ]["ReadoutArea"]
        pixel_size = gs_xml_data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
            "numericValue"
        ]
        full_size = (int(readout_area["a:width"]), int(readout_area["a:height"]))
        return GridSquare(
            id=grid_square,
            session_id=session_id,
            readout_area_x=full_size[0] if image_path else None,
            readout_area_y=full_size[1] if image_path else None,
            thumbnail_size_x=None,
            thumbnail_size_y=None,
            pixel_size=float(pixel_size) if image_path else None,
            image=str(image_path),
        )
    return GridSquare(id=grid_square, session_id=session_id)


def _foil_hole_data(
    xml_path: Path, foil_hole: int, grid_square: int, session_id: int
) -> FoilHole:
    with open(xml_path, "r") as xml:
        for_parsing = xml.read()
        data = xmltodict.parse(for_parsing)
    data = data["GridSquareXml"]
    serialization_array = data["TargetLocations"]["TargetLocationsEfficient"][
        "a:m_serializationArray"
    ]
    required_key = ""
    for key in serialization_array.keys():
        if key.startswith("b:KeyValuePairOfintTargetLocation"):
            required_key = key
            break
    if required_key:
        image_paths = list(
            (xml_path.parent.parent).glob(
                f"Images-Disc*/GridSquare_{grid_square}/FoilHoles/FoilHole_{foil_hole}_*.jpg"
            )
        )
        image_paths.sort(key=lambda x: x.stat().st_ctime)
        image_path: Path | str = image_paths[-1] if image_paths else ""
        if image_path:
            with open(Path(image_path).with_suffix(".xml")) as fh_xml:
                fh_xml_data = xmltodict.parse(fh_xml.read())
            readout_area = fh_xml_data["MicroscopeImage"]["microscopeData"][
                "acquisition"
            ]["camera"]["ReadoutArea"]
            pixel_size = fh_xml_data["MicroscopeImage"]["SpatialScale"]["pixelSize"][
                "x"
            ]["numericValue"]
            full_size = (int(readout_area["a:width"]), int(readout_area["a:height"]))
        for fh_block in serialization_array[required_key]:
            pix = fh_block["b:value"]["PixelCenter"]
            stage = fh_block["b:value"]["StagePosition"]
            if int(fh_block["b:key"]) == foil_hole:
                return FoilHole(
                    id=foil_hole,
                    grid_square_id=grid_square,
                    session_id=session_id,
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
                )
    logger.warning(
        f"Foil hole positions could not be determined from metadata file {xml_path} for foil hole {foil_hole}"
    )
    return FoilHole(id=foil_hole, grid_square_id=grid_square, session_id=session_id)


def _get_source(file_path: Path, environment: MurfeyInstanceEnvironment) -> Path | None:
    for s in environment.sources:
        if file_path.is_relative_to(s):
            return s
    return None


def _get_xml_list_index(key: str, xml_list: list) -> int:
    for i, elem in enumerate(xml_list):
        if elem["a:Key"] == key:
            return i
    raise ValueError(f"Key not found in XML list: {key}")


class _SPAContext(Context):
    user_params = [
        ProcessingParameter(
            "dose_per_frame",
            "Dose Per Frame [e- / Angstrom^2 / frame] (after EER grouping if relevant)",
            default=1,
        ),
        ProcessingParameter(
            "estimate_particle_diameter",
            "Use crYOLO to Estimate Particle Diameter",
            default=True,
        ),
        ProcessingParameter(
            "particle_diameter", "Particle Diameter (Angstroms)", default=None
        ),
        ProcessingParameter("use_cryolo", "Use crYOLO Autopicking", default=True),
        ProcessingParameter("symmetry", "Symmetry Group", default="C1"),
        ProcessingParameter("eer_fractionation", "EER Fractionation", default=20),
        ProcessingParameter(
            "mask_diameter", "Mask Diameter (2D classification)", default=190
        ),
        ProcessingParameter("boxsize", "Box Size", default=256),
        ProcessingParameter("downscale", "Downscale Extracted Particles", default=True),
        ProcessingParameter(
            "small_boxsize", "Downscaled Extracted Particle Size (pixels)", default=128
        ),
        ProcessingParameter("gain_ref", "Gain Reference"),
        ProcessingParameter("gain_ref_superres", "Unbinned Gain Reference"),
    ]
    metadata_params = [
        ProcessingParameter("voltage", "Voltage"),
        ProcessingParameter("image_size_x", "Image Size X"),
        ProcessingParameter("image_size_y", "Image Size Y"),
        ProcessingParameter("pixel_size_on_image", "Pixel Size"),
        ProcessingParameter("motion_corr_binning", "Motion Correction Binning"),
    ]

    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("SPA", acquisition_software)
        self._basepath = basepath
        self._processing_job_stash: dict = {}
        self._preprocessing_triggers: dict = {}
        self._foil_holes: Dict[int, List[int]] = {}

    def gather_metadata(
        self, metadata_file: Path, environment: MurfeyInstanceEnvironment | None = None
    ):
        logger.info(f"trying to gather metadata on {metadata_file}")
        if metadata_file.suffix != ".xml":
            raise ValueError(
                f"SPA gather_metadata method expected xml file not {metadata_file.name}"
            )
        if not metadata_file.is_file():
            logger.debug(f"Metadata file {metadata_file} not found")
            return OrderedDict({})
        with open(metadata_file, "r") as xml:
            try:
                for_parsing = xml.read()
            except Exception:
                logger.warning(f"Failed to parse file {metadata_file}")
                return OrderedDict({})
            data = xmltodict.parse(for_parsing)
        magnification = 0
        num_fractions = 1
        metadata: OrderedDict = OrderedDict({})
        metadata["experiment_type"] = "SPA"
        if data.get("Acquisition"):
            metadata["voltage"] = 300
            metadata["image_size_x"] = data["Acquisition"]["Info"]["ImageSize"]["Width"]
            metadata["image_size_y"] = data["Acquisition"]["Info"]["ImageSize"][
                "Height"
            ]
            metadata["pixel_size_on_image"] = float(
                data["Acquisition"]["Info"]["SensorPixelSize"]["Height"]
            )
            metadata["magnification"] = magnification
        elif data.get("MicroscopeImage"):
            metadata["voltage"] = (
                float(
                    data["MicroscopeImage"]["microscopeData"]["gun"][
                        "AccelerationVoltage"
                    ]
                )
                / 1000
            )
            metadata["image_size_x"] = data["MicroscopeImage"]["microscopeData"][
                "acquisition"
            ]["camera"]["ReadoutArea"]["a:width"]
            metadata["image_size_y"] = data["MicroscopeImage"]["microscopeData"][
                "acquisition"
            ]["camera"]["ReadoutArea"]["a:height"]
            metadata["pixel_size_on_image"] = float(
                data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
                    "numericValue"
                ]
            )
            magnification = data["MicroscopeImage"]["microscopeData"]["optics"][
                "TemMagnification"
            ]["NominalMagnification"]
            metadata["magnification"] = magnification
            try:
                dose_index = _get_xml_list_index(
                    "Dose",
                    data["MicroscopeImage"]["CustomData"]["a:KeyValueOfstringanyType"],
                )
                metadata["total_exposed_dose"] = round(
                    float(
                        data["MicroscopeImage"]["CustomData"][
                            "a:KeyValueOfstringanyType"
                        ][dose_index]["a:Value"]["#text"]
                    )
                    * (1e-20),
                    2,
                )  # convert e / m^2 to e / A^2
            except ValueError:
                metadata["total_exposed_dose"] = 1
            try:
                num_fractions = int(
                    data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"][
                        "CameraSpecificInput"
                    ]["a:KeyValueOfstringanyType"][2]["a:Value"]["b:NumberOffractions"]
                )
            except (KeyError, IndexError):
                pass
            c2_index = 3
            for i, el in enumerate(
                data["MicroscopeImage"]["CustomData"]["a:KeyValueOfstringanyType"]
            ):
                if el["a:Key"] == "Aperture[C2].Name":
                    c2_index = i
                    break
            metadata["c2aperture"] = data["MicroscopeImage"]["CustomData"][
                "a:KeyValueOfstringanyType"
            ][c2_index]["a:Value"]["#text"]
            metadata["exposure_time"] = data["MicroscopeImage"]["microscopeData"][
                "acquisition"
            ]["camera"]["ExposureTime"]
            try:
                metadata["slit_width"] = data["MicroscopeImage"]["microscopeData"][
                    "optics"
                ]["EnergyFilter"]["EnergySelectionSlitWidth"]
            except KeyError:
                metadata["slit_width"] = None
            metadata["phase_plate"] = (
                1
                if data["MicroscopeImage"]["CustomData"]["a:KeyValueOfstringanyType"][
                    11
                ]["a:Value"]["#text"]
                == "true"
                else 0
            )
        else:
            logger.warning("Metadata file format is not recognised")
            return OrderedDict({})
        binning_factor_xml = int(
            data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"][
                "Binning"
            ]["a:x"]
        )
        binning_factor = 1
        if environment:
            server_config_response = capture_get(
                f"{str(environment.url.geturl())}/machine/"
            )
            if server_config_response is None:
                return None
            server_config = server_config_response.json()
            if server_config.get("superres") and not environment.superres:
                # If camera is capable of superres and collection is in superres
                binning_factor = 2
            elif not server_config.get("superres"):
                binning_factor_xml = 2
            if magnification:
                ps_from_mag = (
                    server_config.get("calibrations", {})
                    .get("magnification", {})
                    .get(magnification)
                )
                if ps_from_mag:
                    metadata["pixel_size_on_image"] = float(ps_from_mag) * 1e-10
                    # this is a bit of a hack to cover the case when the data is binned K3
                    # then the pixel size from the magnification table will be correct but the binning_factor will be 2
                    # this is divided out later so multiply it in here to cancel
                    if server_config.get("superres") and not environment.superres:
                        metadata["pixel_size_on_image"] /= (
                            1 if binning_factor_xml == 2 else 2
                        )
                else:
                    metadata["pixel_size_on_image"] /= binning_factor
        metadata["image_size_x"] = str(int(metadata["image_size_x"]) * binning_factor)
        metadata["image_size_y"] = str(int(metadata["image_size_y"]) * binning_factor)
        metadata["motion_corr_binning"] = 1 if binning_factor_xml == 2 else 2
        if environment:
            metadata["gain_ref"] = (
                environment.data_collection_parameters.get("gain_ref")
                if environment
                and environment.data_collection_parameters.get("gain_ref")
                not in (None, "None")
                else f"data/{datetime.now().year}/{environment.visit}/processing/gain.mrc"
            )
            metadata["gain_ref_superres"] = (
                environment.data_collection_parameters.get("gain_ref_superres")
                if environment
                and environment.data_collection_parameters.get("gain_ref_superres")
                not in (None, "None")
                else f"data/{datetime.now().year}/{environment.visit}/processing/gain_superres.mrc"
            )
        else:
            metadata["gain_ref"] = None
            metadata["gain_ref_superres"] = None
        if metadata.get("total_exposed_dose"):
            metadata["dose_per_frame"] = (
                environment.data_collection_parameters.get("dose_per_frame")
                if environment
                and environment.data_collection_parameters.get("dose_per_frame")
                not in (None, "None")
                else round(metadata["total_exposed_dose"] / num_fractions, 3)
            )
        else:
            metadata["dose_per_frame"] = (
                environment.data_collection_parameters.get("dose_per_frame")
                if environment
                else None
            )

        metadata["use_cryolo"] = (
            environment.data_collection_parameters.get("use_cryolo")
            if environment
            else None
        ) or True
        metadata["symmetry"] = (
            environment.data_collection_parameters.get("symmetry")
            if environment
            else None
        ) or "C1"
        metadata["mask_diameter"] = (
            environment.data_collection_parameters.get("mask_diameter")
            if environment
            else None
        ) or 190
        metadata["boxsize"] = (
            environment.data_collection_parameters.get("boxsize")
            if environment
            else None
        ) or 256
        metadata["downscale"] = (
            environment.data_collection_parameters.get("downscale")
            if environment
            else None
        ) or True
        metadata["small_boxsize"] = (
            environment.data_collection_parameters.get("small_boxsize")
            if environment
            else None
        ) or 128
        metadata["eer_fractionation"] = (
            environment.data_collection_parameters.get("eer_fractionation")
            if environment
            else None
        ) or 20
        metadata["source"] = str(self._basepath)
        metadata["particle_diameter"] = (
            environment.data_collection_parameters.get("particle_diameter")
            if environment
            else None
        ) or 0
        metadata["estimate_particle_diameter"] = (
            environment.data_collection_parameters.get("estimate_particle_diameter")
            if environment
            else None
        ) or True
        return metadata


class SPAModularContext(_SPAContext):
    def _position_analysis(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment,
        source: Path,
        machine_config: dict,
    ) -> int:
        grid_square = _grid_square_from_file(transferred_file)
        grid_square_metadata_file = _grid_square_metadata_file(
            transferred_file,
            {Path(d): l for d, l in machine_config["data_directories"].items()},
            environment.visit,
            grid_square,
        )
        if (
            grid_square is not None
            and environment.murfey_session is not None
            and self._foil_holes.get(grid_square) is None
        ):
            self._foil_holes[grid_square] = []
            gs_pix_position: Tuple[
                Optional[int],
                Optional[int],
                Optional[float],
                Optional[float],
            ] = (None, None, None, None)
            data_collection_group = (
                requests.get(
                    f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/data_collection_groups"
                )
                .json()
                .get(str(source), {})
            )
            if data_collection_group.get("atlas"):
                visit_path = ""
                for p in transferred_file.parts:
                    if p == environment.visit:
                        break
                    visit_path += f"/{p}"
                if source in list(environment.samples.keys()):
                    local_atlas_path = (
                        Path(visit_path) / environment.samples[source].atlas
                    )
                    gs_pix_position = _get_grid_square_atlas_positions(
                        local_atlas_path,
                        grid_square=str(grid_square),
                    )[str(grid_square)]
            gs_url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/grid_square/{grid_square}"
            gs = _grid_square_data(
                grid_square_metadata_file,
                grid_square,
                environment.murfey_session,
            )
            metadata_source = Path(
                (
                    "/".join(source.parts[:-2])
                    + f"/{environment.visit}/"
                    + source.parts[-2]
                )[1:]
            )
            image_path = (
                _file_transferred_to(environment, metadata_source, Path(gs.image))
                if gs.image
                else ""
            )
            capture_post(
                gs_url,
                json={
                    "tag": str(source),
                    "readout_area_x": gs.readout_area_x,
                    "readout_area_y": gs.readout_area_y,
                    "thumbnail_size_x": gs.thumbnail_size_x,
                    "thumbnail_size_y": gs.thumbnail_size_y,
                    "pixel_size": gs.pixel_size,
                    "image": str(image_path),
                    "x_location": gs_pix_position[0],
                    "y_location": gs_pix_position[1],
                    "x_stage_position": gs_pix_position[2],
                    "y_stage_position": gs_pix_position[3],
                },
            )
        foil_hole = _foil_hole_from_file(transferred_file)
        if foil_hole not in self._foil_holes[grid_square]:
            fh_url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/grid_square/{grid_square}/foil_hole"
            if (
                grid_square_metadata_file.is_file()
                and environment.murfey_session is not None
            ):
                fh = _foil_hole_data(
                    grid_square_metadata_file,
                    foil_hole,
                    grid_square,
                    environment.murfey_session,
                )
                metadata_source = Path(
                    (
                        "/".join(source.parts[:-2])
                        + f"/{environment.visit}/"
                        + source.parts[-2]
                    )[1:]
                )
                image_path = (
                    _file_transferred_to(environment, metadata_source, Path(fh.image))
                    if fh.image
                    else ""
                )
                capture_post(
                    fh_url,
                    json={
                        "name": foil_hole,
                        "x_location": fh.x_location,
                        "y_location": fh.y_location,
                        "x_stage_position": fh.x_stage_position,
                        "y_stage_position": fh.y_stage_position,
                        "readout_area_x": fh.readout_area_x,
                        "readout_area_y": fh.readout_area_y,
                        "thumbnail_size_x": fh.thumbnail_size_x,
                        "thumbnail_size_y": fh.thumbnail_size_y,
                        "pixel_size": fh.pixel_size,
                        "tag": str(source),
                        "image": str(image_path),
                    },
                )
            else:
                capture_post(
                    fh_url,
                    json={
                        "name": foil_hole,
                        "tag": str(source),
                    },
                )
            self._foil_holes[grid_square].append(foil_hole)
        return foil_hole

    def post_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ) -> bool:
        data_suffixes = (".mrc", ".tiff", ".tif", ".eer")
        if role == "detector" and "gain" not in transferred_file.name:
            if transferred_file.suffix in data_suffixes:
                if self._acquisition_software == "epu":
                    if environment:
                        machine_config = get_machine_config(
                            str(environment.url.geturl()), demo=environment.demo
                        )
                    else:
                        machine_config = {}
                    required_strings = (
                        machine_config.get("data_required_substrings", {})
                        .get("epu", {})
                        .get(transferred_file.suffix, ["fractions"])
                    )

                    if not environment:
                        logger.warning("No environment passed in")
                        return True
                    source = _get_source(transferred_file, environment)
                    if not source:
                        logger.warning(f"No source found for file {transferred_file}")
                        return True

                    if required_strings and not any(
                        r in transferred_file.name for r in required_strings
                    ):
                        return True

                    if environment:
                        file_transferred_to = _file_transferred_to(
                            environment, source, transferred_file
                        )
                        if not environment.movie_counters.get(str(source)):
                            movie_counts_get = capture_get(
                                f"{str(environment.url.geturl())}/num_movies",
                            )
                            if movie_counts_get is not None:
                                environment.movie_counters[str(source)] = count(
                                    movie_counts_get.json().get(str(source), 0) + 1
                                )
                        environment.movies[file_transferred_to] = MovieTracker(
                            movie_number=next(environment.movie_counters[str(source)]),
                            motion_correction_uuid=next(MurfeyID),
                        )

                        eer_fractionation_file = None
                        if file_transferred_to.suffix == ".eer":
                            response = capture_post(
                                f"{str(environment.url.geturl())}/visits/{environment.visit}/eer_fractionation_file",
                                json={
                                    "eer_path": str(file_transferred_to),
                                    "fractionation": environment.data_collection_parameters[
                                        "eer_fractionation"
                                    ],
                                    "dose_per_frame": environment.data_collection_parameters[
                                        "dose_per_frame"
                                    ],
                                    "fractionation_file_name": "eer_fractionation_spa.txt",
                                },
                            )
                            if response is None:
                                return False
                            eer_fractionation_file = response.json()[
                                "eer_fractionation_file"
                            ]

                        try:
                            foil_hole: Optional[int] = self._position_analysis(
                                transferred_file, environment, source, machine_config
                            )
                        except Exception:
                            # try to continue if position information gathering fails so that movie is processed anyway
                            foil_hole = None

                        preproc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.client_id}/spa_preprocess"
                        preproc_data = {
                            "path": str(file_transferred_to),
                            "description": "",
                            "processing_job": None,
                            "data_collection_id": None,
                            "image_number": environment.movies[
                                file_transferred_to
                            ].movie_number,
                            "pixel_size": environment.data_collection_parameters.get(
                                "pixel_size_on_image"
                            ),
                            "autoproc_program_id": None,
                            "dose_per_frame": environment.data_collection_parameters.get(
                                "dose_per_frame"
                            ),
                            "mc_binning": environment.data_collection_parameters.get(
                                "motion_corr_binning", 1
                            ),
                            "gain_ref": environment.data_collection_parameters.get(
                                "gain_ref"
                            ),
                            "extract_downscale": environment.data_collection_parameters.get(
                                "downscale"
                            ),
                            "eer_fractionation_file": eer_fractionation_file,
                            "tag": str(source),
                            "foil_hole_id": foil_hole,
                        }
                        capture_post(
                            preproc_url,
                            json={
                                k: None if v == "None" else v
                                for k, v in preproc_data.items()
                            },
                        )

        return True

    def _register_data_collection(
        self,
        tag: str,
        url: str,
        data: dict,
        environment: MurfeyInstanceEnvironment,
    ):
        return

    def _register_processing_job(
        self,
        tag: str,
        environment: MurfeyInstanceEnvironment,
        parameters: Dict[str, Any] | None = None,
    ):
        return

    def _launch_spa_pipeline(
        self,
        tag: str,
        jobid: int,
        environment: MurfeyInstanceEnvironment,
        url: str = "",
    ):
        return


class SPAContext(_SPAContext):
    def _register_data_collection(
        self,
        tag: str,
        url: str,
        data: dict,
        environment: MurfeyInstanceEnvironment,
    ):
        logger.info(f"registering data collection with data {data}")
        environment.id_tag_registry["data_collection"].append(tag)
        image_directory = str(environment.default_destinations[Path(tag)])
        json = {
            "voltage": data["voltage"],
            "pixel_size_on_image": data["pixel_size_on_image"],
            "experiment_type": data["experiment_type"],
            "image_size_x": data["image_size_x"],
            "image_size_y": data["image_size_y"],
            "file_extension": data["file_extension"],
            "acquisition_software": data["acquisition_software"],
            "image_directory": image_directory,
            "tag": tag,
            "source": tag,
            "magnification": data["magnification"],
            "total_exposed_dose": data.get("total_exposed_dose"),
            "c2aperture": data.get("c2aperture"),
            "exposure_time": data.get("exposure_time"),
            "slit_width": data.get("slit_width"),
            "phase_plate": data.get("phase_plate", False),
        }
        capture_post(url, json=json)

    def post_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ) -> bool:
        return True

    def _register_processing_job(
        self,
        tag: str,
        environment: MurfeyInstanceEnvironment,
        parameters: Dict[str, Any] | None = None,
    ):
        logger.info(f"registering processing job with parameters: {parameters}")
        parameters = parameters or {}
        environment.id_tag_registry["processing_job"].append(tag)
        proc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.client_id}/register_processing_job"
        machine_config = get_machine_config(
            str(environment.url.geturl()), demo=environment.demo
        )
        image_directory = str(
            Path(machine_config.get("rsync_basepath", "."))
            / environment.default_destinations[Path(tag)]
        )
        if self._acquisition_software == "epu":
            import_images = f"{Path(image_directory).resolve()}/GridSquare*/Data/*{parameters['file_extension']}"
        else:
            import_images = (
                f"{Path(image_directory).resolve()}/*{parameters['file_extension']}"
            )
        msg: Dict[str, Any] = {
            "tag": tag,
            "recipe": "ispyb-relion",
            "parameters": {
                "acquisition_software": parameters["acquisition_software"],
                "voltage": parameters["voltage"],
                "gain_ref": parameters["gain_ref"],
                "dose_per_frame": parameters["dose_per_frame"],
                "eer_grouping": parameters["eer_fractionation"],
                "import_images": import_images,
                "angpix": float(parameters["pixel_size_on_image"]) * 1e10,
                "symmetry": parameters["symmetry"],
                "boxsize": parameters["boxsize"],
                "downscale": parameters["downscale"],
                "small_boxsize": parameters["small_boxsize"],
                "mask_diameter": parameters["mask_diameter"],
                "use_cryolo": parameters["use_cryolo"],
                "estimate_particle_diameter": parameters["estimate_particle_diameter"],
            },
        }
        if parameters["particle_diameter"]:
            msg["parameters"]["particle_diameter"] = parameters["particle_diameter"]
        capture_post(proc_url, json=msg)

    def _launch_spa_pipeline(
        self,
        tag: str,
        jobid: int,
        environment: MurfeyInstanceEnvironment,
        url: str = "",
    ):
        environment.id_tag_registry["auto_proc_program"].append(tag)
        data = {"job_id": jobid}
        capture_post(url, json=data)
