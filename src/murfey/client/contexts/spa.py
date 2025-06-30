from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from typing import Any, Dict, List, Optional, OrderedDict, Tuple

import requests
import xmltodict

from murfey.client.context import Context, ProcessingParameter
from murfey.client.instance_environment import (
    MovieTracker,
    MurfeyID,
    MurfeyInstanceEnvironment,
)
from murfey.util.api import url_path_for
from murfey.util.client import (
    authorised_requests,
    capture_get,
    capture_post,
    get_machine_config_client,
)
from murfey.util.spa_metadata import (
    foil_hole_data,
    foil_hole_from_file,
    get_grid_square_atlas_positions,
    grid_square_data,
    grid_square_from_file,
)

logger = logging.getLogger("murfey.client.contexts.spa")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
):
    machine_config = get_machine_config_client(
        str(environment.url.geturl()),
        instrument_name=environment.instrument_name,
        demo=environment.demo,
    )
    if environment.visit in environment.default_destinations[source]:
        return (
            Path(machine_config.get("rsync_basepath", ""))
            / Path(environment.default_destinations[source])
            / file_path.relative_to(source)  # need to strip out the rsync_module name
        )
    return (
        Path(machine_config.get("rsync_basepath", ""))
        / Path(environment.default_destinations[source])
        / environment.visit
        / file_path.relative_to(source)
    )


def _grid_square_metadata_file(
    f: Path, data_directories: List[Path], visit: str, grid_square: int
) -> Path:
    for dd in data_directories:
        if str(f).startswith(str(dd)):
            base_dir = dd.absolute()
            mid_dir = f.relative_to(base_dir).parent
            break
    else:
        raise ValueError(f"Could not determine grid square metadata path for {f}")
    metadata_file = (
        base_dir
        / visit
        / mid_dir.parent.parent.parent
        / "Metadata"
        / f"GridSquare_{grid_square}.dm"
    )
    if not metadata_file.is_file():
        logger.warning(f"Grid square metadata file {str(metadata_file)} does not exist")
    return metadata_file


def _get_source(file_path: Path, environment: MurfeyInstanceEnvironment) -> Path | None:
    possible_sources = []
    for s in environment.sources:
        if file_path.is_relative_to(s):
            possible_sources.append(s)
    if not possible_sources:
        return None
    elif len(possible_sources) == 1:
        return possible_sources[0]
    source = possible_sources[0]
    for extra_source in possible_sources[1:]:
        if extra_source.is_relative_to(source):
            source = extra_source
    return source


def _get_xml_list_index(key: str, xml_list: list) -> int:
    for i, elem in enumerate(xml_list):
        if elem["a:Key"] == key:
            return i
    raise ValueError(f"Key not found in XML list: {key}")


class SPAModularContext(Context):
    user_params = [
        ProcessingParameter(
            "dose_per_frame",
            "Dose Per Frame [e- / Angstrom^2 / frame] (after EER grouping if relevant)",
            default=1,
        ),
        ProcessingParameter("symmetry", "Symmetry Group", default="C1"),
        ProcessingParameter("eer_fractionation", "EER Fractionation", default=20),
        ProcessingParameter("gain_ref", "Gain Reference"),
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
                f"{str(environment.url.geturl())}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=environment.instrument_name)}"
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
        metadata["gain_ref"] = environment.gain_ref if environment else None
        metadata["dose_per_frame"] = environment.dose_per_frame if environment else None
        metadata["symmetry"] = (environment.symmetry if environment else None) or "C1"
        metadata["eer_fractionation"] = (
            environment.eer_fractionation if environment else None
        ) or 20
        metadata["source"] = str(self._basepath)
        return metadata

    def _position_analysis(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment,
        source: Path,
        machine_config: dict,
    ) -> Optional[int]:
        grid_square = grid_square_from_file(transferred_file)
        grid_square_metadata_file = _grid_square_metadata_file(
            transferred_file,
            [Path(p) for p in machine_config["data_directories"]],
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
                Optional[int],
                Optional[int],
                Optional[float],
            ] = (None, None, None, None, None, None, None)
            data_collection_group = (
                requests.get(
                    f"{environment.url.geturl()}{url_path_for('session_info.router', 'get_dc_groups', session_id=environment.murfey_session)}"
                )
                .json()
                .get(str(source), {})
            )
            if not data_collection_group:
                logger.info("Data collection group has not yet been made")
                return None
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
                    gs_pix_position = get_grid_square_atlas_positions(
                        local_atlas_path,
                        grid_square=str(grid_square),
                    )[str(grid_square)]
            gs_url = f"{str(environment.url.geturl())}{url_path_for('session_control.spa_router', 'register_grid_square', session_id=environment.murfey_session, gsid=grid_square)}"
            gs = grid_square_data(
                grid_square_metadata_file,
                grid_square,
            )
            metadata_source_as_str = (
                "/".join(source.parts[:-2])
                + f"/{environment.visit}/"
                + source.parts[-2]
            )
            metadata_source = Path(
                metadata_source_as_str[1:]
                if metadata_source_as_str.startswith("//")
                else metadata_source_as_str
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
                    "width": gs_pix_position[4],
                    "height": gs_pix_position[5],
                    "angle": gs_pix_position[6],
                },
            )
        foil_hole = foil_hole_from_file(transferred_file)
        if foil_hole not in self._foil_holes[grid_square]:
            fh_url = f"{str(environment.url.geturl())}{url_path_for('session_control.spa_router', 'register_foil_hole', session_id=environment.murfey_session, gs_name=grid_square)}"
            if environment.murfey_session is not None:
                fh = foil_hole_data(
                    grid_square_metadata_file,
                    foil_hole,
                    grid_square,
                )
                metadata_source_as_str = (
                    "/".join(source.parts[:-2])
                    + f"/{environment.visit}/"
                    + source.parts[-2]
                )
                metadata_source = Path(
                    metadata_source_as_str[1:]
                    if metadata_source_as_str.startswith("//")
                    else metadata_source_as_str
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
                        "diameter": fh.diameter,
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
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ) -> bool:
        super().post_transfer(
            transferred_file=transferred_file,
            environment=environment,
            **kwargs,
        )
        data_suffixes = (".mrc", ".tiff", ".tif", ".eer")
        if "gain" not in transferred_file.name:
            if transferred_file.suffix in data_suffixes:
                if self._acquisition_software == "epu":
                    if environment:
                        machine_config = get_machine_config_client(
                            str(environment.url.geturl()),
                            instrument_name=environment.instrument_name,
                            demo=environment.demo,
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
                                f"{environment.url.geturl()}{url_path_for('session_control.router', 'count_number_of_movies')}",
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
                                f"{str(environment.url.geturl())}{url_path_for('file_io_instrument.router', 'write_eer_fractionation_file', visit_name=environment.visit, session_id=environment.murfey_session)}",
                                json={
                                    "eer_path": str(file_transferred_to),
                                    "fractionation": self.data_collection_parameters[
                                        "eer_fractionation"
                                    ],
                                    "dose_per_frame": self.data_collection_parameters[
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
                        except Exception as e:
                            # try to continue if position information gathering fails so that movie is processed anyway
                            logger.warning(
                                f"Unable to register foil hole for {str(file_transferred_to)}. Exception: {str(e)}",
                                exc_info=True,
                            )
                            foil_hole = None

                        preproc_url = f"{str(environment.url.geturl())}{url_path_for('workflow.spa_router', 'request_spa_preprocessing', visit_name=environment.visit, session_id=environment.murfey_session)}"
                        preproc_data = {
                            "path": str(file_transferred_to),
                            "description": "",
                            "processing_job": None,
                            "data_collection_id": None,
                            "image_number": environment.movies[
                                file_transferred_to
                            ].movie_number,
                            "pixel_size": self.data_collection_parameters.get(
                                "pixel_size_on_image"
                            ),
                            "autoproc_program_id": None,
                            "dose_per_frame": environment.dose_per_frame,
                            "mc_binning": self.data_collection_parameters.get(
                                "motion_corr_binning", 1
                            ),
                            "gain_ref": environment.gain_ref,
                            "extract_downscale": self.data_collection_parameters.get(
                                "downscale", True
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
