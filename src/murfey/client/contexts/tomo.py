from __future__ import annotations

import logging
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, List, OrderedDict

import requests
import xmltodict

import murfey.util.eer
from murfey.client.context import Context, ProcessingParameter
from murfey.client.instance_environment import (
    MovieID,
    MovieTracker,
    MurfeyID,
    MurfeyInstanceEnvironment,
)
from murfey.util.api import url_path_for
from murfey.util.client import (
    authorised_requests,
    capture_post,
    get_machine_config_client,
)
from murfey.util.mdoc import get_block, get_global_data, get_num_blocks

logger = logging.getLogger("murfey.client.contexts.tomo")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


def _get_tilt_angle_v5_7(p: Path) -> str:
    return p.name.split("[")[1].split("]")[0]


def _get_tilt_angle_v5_11(p: Path) -> str:
    _split = p.name.split("_")[2].split(".")
    return ".".join(_split[:-1])


def _find_angle_index(split_name: List[str]) -> int:
    for i, part in enumerate(split_name):
        if "." in part and part[-1].isnumeric():
            return i
    return -1


def _get_tilt_angle_v5_12(p: Path) -> str:
    split_name = p.stem.split("_")
    angle_idx = _find_angle_index(split_name)
    if angle_idx == -1:
        return ""
    return split_name[angle_idx]


tomo_tilt_info = {
    "5.7": _get_tilt_angle_v5_7,
    "5.11": _get_tilt_angle_v5_11,
    "5.12": _get_tilt_angle_v5_12,
}


def _construct_tilt_series_name(file_path: Path) -> str:
    # Assuming files end with _{tiltnumber}_{angle}_{date}_{time}_{fractions}.{suffix}
    split_name = file_path.name.split("_")
    return "_".join(split_name[:-5])


class TomographyContext(Context):
    user_params = [
        ProcessingParameter(
            "dose_per_frame", "Dose Per Frame (e- / Angstrom^2 / frame)", default=1
        ),
        ProcessingParameter("gain_ref", "Gain Reference"),
        ProcessingParameter("eer_fractionation", "EER Fractionation", default=20),
    ]
    metadata_params = [
        ProcessingParameter("voltage", "Voltage"),
        ProcessingParameter("image_size_x", "Image Size X"),
        ProcessingParameter("image_size_y", "Image Size Y"),
        ProcessingParameter("pixel_size_on_image", "Pixel Size"),
        ProcessingParameter("motion_corr_binning", "Motion Correction Binning"),
        ProcessingParameter("frame_count", "Number of image frames"),
        ProcessingParameter("tilt_axis", "Stage rotation angle"),
        ProcessingParameter("num_eer_frames", "Number of EER Frames"),
    ]

    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("Tomography", acquisition_software)
        self._basepath = basepath
        self._tilt_series: Dict[str, List[Path]] = {}
        self._tilt_series_with_pjids: List[str] = []
        self._tilt_series_sizes: Dict[str, int] = {}
        self._completed_tilt_series: List[str] = []
        self._aligned_tilt_series: List[str] = []
        self._data_collection_stash: list = []
        self._processing_job_stash: dict = {}
        self._lock: RLock = RLock()

    def register_tomography_data_collections(
        self,
        file_extension: str,
        image_directory: str,
        environment: MurfeyInstanceEnvironment | None = None,
    ):
        if not environment:
            logger.error(
                "No environment passed to register tomography data collections"
            )
            return
        try:
            dcg_url = f"{str(environment.url.geturl())}{url_path_for('workflow.router', 'register_dc_group', visit_name=environment.visit, session_id=environment.murfey_session)}"
            dcg_data = {
                "experiment_type": "tomo",
                "experiment_type_id": 36,
                "tag": str(self._basepath),
                "atlas": "",
                "sample": None,
            }
            capture_post(dcg_url, json=dcg_data)

            for tilt_series in self._tilt_series.keys():
                if tilt_series not in self._tilt_series_with_pjids:
                    dc_url = f"{str(environment.url.geturl())}{url_path_for('workflow.router', 'start_dc', visit_name=environment.visit, session_id=environment.murfey_session)}"
                    dc_data = {
                        "experiment_type": "tomography",
                        "file_extension": file_extension,
                        "acquisition_software": self._acquisition_software,
                        "image_directory": image_directory,
                        "data_collection_tag": tilt_series,
                        "source": str(self._basepath),
                        "tag": tilt_series,
                    }
                    if (
                        self.data_collection_parameters
                        and self.data_collection_parameters.get("voltage")
                    ):
                        # Once mdoc parameters are known register processing jobs
                        dc_data.update(
                            {
                                "voltage": self.data_collection_parameters["voltage"],
                                "pixel_size_on_image": self.data_collection_parameters[
                                    "pixel_size_on_image"
                                ],
                                "image_size_x": self.data_collection_parameters[
                                    "image_size_x"
                                ],
                                "image_size_y": self.data_collection_parameters[
                                    "image_size_y"
                                ],
                                "magnification": self.data_collection_parameters[
                                    "magnification"
                                ],
                            }
                        )
                        capture_post(dc_url, json=dc_data)

                        proc_url = f"{str(environment.url.geturl())}{url_path_for('workflow.router', 'register_proc', visit_name=environment.visit, session_id=environment.murfey_session)}"
                        for recipe in ("em-tomo-preprocess", "em-tomo-align"):
                            capture_post(
                                proc_url,
                                json={
                                    "tag": tilt_series,
                                    "source": str(self._basepath),
                                    "recipe": recipe,
                                    "experiment_type": "tomography",
                                },
                            )
                        self._tilt_series_with_pjids.append(tilt_series)
                    else:
                        logger.info(
                            "Cannot register data collection yet as no values from mdoc"
                        )

        except Exception as e:
            logger.error(f"ERROR {e}, {self.data_collection_parameters}", exc_info=True)

    def _file_transferred_to(
        self, environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
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
                / file_path.name
            )
        return (
            Path(machine_config.get("rsync_basepath", ""))
            / Path(environment.default_destinations[source])
            / environment.visit
            / file_path.name
        )

    def _get_source(
        self, file_path: Path, environment: MurfeyInstanceEnvironment
    ) -> Path | None:
        for s in environment.sources:
            if file_path.is_relative_to(s):
                return s
        return None

    def _add_tilt(
        self,
        file_path: Path,
        extract_tilt_angle: Callable[[Path], str],
        environment: MurfeyInstanceEnvironment | None = None,
        required_strings: List[str] | None = None,
    ) -> List[str]:
        if not environment:
            logger.warning("No environment passed in")
            return []
        source = self._get_source(file_path, environment)
        if not source:
            logger.warning(f"No source found for file {file_path}")
            return []
        required_strings = (
            ["fractions"] if required_strings is None else required_strings
        )
        if required_strings and not any(r in file_path.name for r in required_strings):
            return []
        try:
            tilt_angle = extract_tilt_angle(file_path)
            try:
                float(tilt_angle)
            except ValueError:
                logger.error(f"whoops, {tilt_angle}")
                return []
            tilt_series = _construct_tilt_series_name(file_path)

        except Exception:
            logger.debug(
                f"Tilt series and angle could not be determined for {file_path}"
            )
            return []

        if environment:
            file_transferred_to = self._file_transferred_to(
                environment, source, file_path
            )
            environment.movies[file_transferred_to] = MovieTracker(
                movie_number=next(MovieID),
                motion_correction_uuid=next(MurfeyID),
            )
            environment.movie_tilt_pair[file_transferred_to] = tilt_series
            if environment.tilt_angles.get(tilt_series):
                environment.tilt_angles[tilt_series].append(
                    [str(file_transferred_to), tilt_angle]
                )
            else:
                environment.tilt_angles[tilt_series] = [
                    [str(file_transferred_to), tilt_angle]
                ]
        if tilt_series in self._completed_tilt_series:
            logger.info(
                f"Tilt series {tilt_series} was previously thought complete but now {file_path} has been seen"
            )
            self._completed_tilt_series.remove(tilt_series)
            rerun_url = f"{str(environment.url.geturl())}{url_path_for('workflow.tomo_router', 'register_tilt_series_for_rerun', visit_name=environment.visit)}"
            rerun_data = {
                "session_id": environment.murfey_session,
                "tag": tilt_series,
                "source": str(file_path.parent),
            }
            capture_post(rerun_url, json=rerun_data)
            if tilt_series in self._aligned_tilt_series:
                with self._lock:
                    self._aligned_tilt_series.remove(tilt_series)

        if not self._tilt_series.get(tilt_series):
            logger.info(f"New tilt series found: {tilt_series}")
            self._tilt_series[tilt_series] = [file_path]
            ts_url = f"{str(environment.url.geturl())}{url_path_for('workflow.tomo_router', 'register_tilt_series', visit_name=environment.visit)}"
            ts_data = {
                "session_id": environment.murfey_session,
                "tag": tilt_series,
                "source": str(file_path.parent),
            }
            capture_post(ts_url, json=ts_data)
            if not self._tilt_series_sizes.get(tilt_series):
                self._tilt_series_sizes[tilt_series] = 0

            # Will register processing jobs for all tilt series except the first one
            self.register_tomography_data_collections(
                file_extension=file_path.suffix,
                image_directory=str(
                    environment.default_destinations.get(
                        file_path.parent, file_path.parent
                    )
                ),
                environment=environment,
            )
        else:
            if file_path not in self._tilt_series[tilt_series]:
                for p in self._tilt_series[tilt_series]:
                    if tilt_angle == extract_tilt_angle(p):
                        break
                else:
                    self._tilt_series[tilt_series].append(file_path)

        if environment:
            tilt_url = f"{str(environment.url.geturl())}{url_path_for('workflow.tomo_router', 'register_tilt', visit_name=environment.visit, session_id=environment.murfey_session)}"
            tilt_data = {
                "movie_path": str(file_transferred_to),
                "tilt_series_tag": tilt_series,
                "source": str(file_path.parent),
            }
            capture_post(tilt_url, json=tilt_data)

            eer_fractionation_file = None
            if self.data_collection_parameters.get("num_eer_frames"):
                response = requests.post(
                    f"{str(environment.url.geturl())}{url_path_for('file_io_instrument.router', 'write_eer_fractionation_file', visit_name=environment.visit, session_id=environment.murfey_session)}",
                    json={
                        "num_frames": self.data_collection_parameters["num_eer_frames"],
                        "fractionation": self.data_collection_parameters[
                            "eer_fractionation"
                        ],
                        "dose_per_frame": environment.dose_per_frame or 0,
                        "fractionation_file_name": "eer_fractionation_tomo.txt",
                    },
                )
                eer_fractionation_file = response.json()["eer_fractionation_file"]
            preproc_url = f"{str(environment.url.geturl())}{url_path_for('workflow.tomo_router', 'request_tomography_preprocessing', visit_name=environment.visit, session_id=environment.murfey_session)}"
            preproc_data = {
                "path": str(file_transferred_to),
                "description": "",
                "image_number": environment.movies[file_transferred_to].movie_number,
                "pixel_size": self.data_collection_parameters.get(
                    "pixel_size_on_image", 0
                ),
                "dose_per_frame": environment.dose_per_frame or 0,
                "frame_count": self.data_collection_parameters.get("frame_count", 0),
                "tilt_axis": self.data_collection_parameters.get("tilt_axis", 85),
                "mc_binning": self.data_collection_parameters.get(
                    "motion_corr_binning", 1
                ),
                "gain_ref": environment.gain_ref,
                "voltage": self.data_collection_parameters.get("voltage", 300),
                "eer_fractionation_file": eer_fractionation_file,
                "tag": tilt_series,
                "group_tag": str(self._basepath),
            }
            capture_post(preproc_url, json=preproc_data)

        return self._check_tilt_series(tilt_series)

    def _check_tilt_series(
        self,
        tilt_series: str,
    ) -> List[str]:
        newly_completed_series: List[str] = []
        mdoc_tilt_series_size = self._tilt_series_sizes.get(tilt_series, 0)
        if not self._tilt_series or not mdoc_tilt_series_size:
            logger.debug(f"Tilt series size not yet set for {tilt_series!r}")
            return newly_completed_series

        counted_tilts = len(self._tilt_series.get(tilt_series, []))
        tilt_series_size_check = counted_tilts >= mdoc_tilt_series_size
        if tilt_series_size_check and tilt_series not in self._completed_tilt_series:
            self._completed_tilt_series.append(tilt_series)
            newly_completed_series.append(tilt_series)
        else:
            logger.debug(
                f"{tilt_series!r} not complete yet. Counted {counted_tilts} tilts. "
                f"Expected number of tilts was {mdoc_tilt_series_size}"
            )
        return newly_completed_series

    def _add_tomo_tilt(
        self,
        file_path: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        required_strings: List[str] | None = None,
    ) -> List[str]:
        required_strings = (
            ["fractions"] if required_strings is None else required_strings
        )
        if not any(r in file_path.name for r in required_strings):
            return []
        if environment:
            if tomo_version := environment.software_versions.get("tomo"):
                tilt_info_extraction = tomo_tilt_info.get(tomo_version)
                if not tilt_info_extraction:
                    raise ValueError(
                        f"Extraction routines for TFS Tomo version {tomo_version} unknown"
                    )
            else:
                tilt_info_extraction = tomo_tilt_info["5.7"]
        else:
            tilt_info_extraction = tomo_tilt_info["5.7"]
        return self._add_tilt(
            file_path,
            tilt_info_extraction,
            environment=environment,
            required_strings=required_strings,
        )

    def _add_serialem_tilt(
        self, file_path: Path, environment: MurfeyInstanceEnvironment | None = None
    ) -> List[str]:
        delimiters = ("_", "-")
        for d in delimiters:
            if file_path.name.count(d) > 1:
                delimiter = d
                break
        else:
            delimiter = delimiters[0]

        def _extract_tilt_series(p: Path) -> str:
            split = p.name.split(delimiter)
            for s in split:
                if s.isdigit():
                    return s
            raise ValueError(
                f"No digits found in {p.name} after splitting on {delimiter}"
            )

        return self._add_tilt(
            file_path,
            lambda x: ".".join(x.name.split(delimiter)[-1].split(".")[:-1]),
            environment=environment,
            required_strings=[],
        )

    def post_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ) -> List[str]:
        super().post_transfer(
            transferred_file=transferred_file,
            environment=environment,
            **kwargs,
        )

        data_suffixes = (".mrc", ".tiff", ".tif", ".eer")
        completed_tilts = []

        if "gain" not in transferred_file.name:
            if transferred_file.suffix in data_suffixes:
                if self._acquisition_software == "tomo":
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
                        .get("tomo", {})
                        .get(transferred_file.suffix, ["fractions"])
                    )
                    completed_tilts = self._add_tomo_tilt(
                        transferred_file,
                        environment=environment,
                        required_strings=kwargs.get("required_strings")
                        or required_strings,
                    )
                elif self._acquisition_software == "serialem":
                    completed_tilts = self._add_serialem_tilt(
                        transferred_file, environment=environment
                    )
            if transferred_file.suffix == ".mdoc":
                with open(transferred_file, "r") as md:
                    tilt_series = transferred_file.stem
                    self._tilt_series_sizes[tilt_series] = get_num_blocks(md)
                if environment:
                    source = self._get_source(transferred_file, environment)
                    if source:
                        completed_tilts = self._check_tilt_series(tilt_series)

                    # Always update the tilt series length in the database after an mdoc
                    if environment.murfey_session is not None:
                        length_url = f"{str(environment.url.geturl())}{url_path_for('workflow.tomo_router', 'register_tilt_series_length', session_id=environment.murfey_session)}"
                        capture_post(
                            length_url,
                            json={
                                "tags": [tilt_series],
                                "source": str(transferred_file.parent),
                                "tilt_series_lengths": [
                                    self._tilt_series_sizes[tilt_series]
                                ],
                            },
                        )

        if completed_tilts and environment:
            logger.info(
                f"The following tilt series are considered complete: {completed_tilts} "
                f"after {transferred_file}"
            )
            complete_url = f"{str(environment.url.geturl())}{url_path_for('workflow.tomo_router', 'register_completed_tilt_series', visit_name=environment.visit, session_id=environment.murfey_session)}"
            capture_post(
                complete_url,
                json={
                    "tags": completed_tilts,
                    "source": str(transferred_file.parent),
                    "tilt_series_lengths": [
                        len(self._tilt_series.get(ts, [])) for ts in completed_tilts
                    ],
                },
            )
        return completed_tilts

    def post_first_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        self.post_transfer(transferred_file, environment=environment, **kwargs)

    def gather_metadata(
        self, metadata_file: Path, environment: MurfeyInstanceEnvironment | None = None
    ) -> OrderedDict:
        if metadata_file.suffix not in (".mdoc", ".xml"):
            raise ValueError(
                f"Tomography gather_metadata method expected xml or mdoc file not {metadata_file.name}"
            )
        try:
            if not metadata_file.is_file():
                logger.debug(f"Metadata file {metadata_file} not found")
                return OrderedDict({})
            if metadata_file.suffix == ".xml":
                with open(metadata_file, "r") as xml:
                    try:
                        for_parsing = xml.read()
                    except Exception:
                        logger.warning(
                            f"Failed to parse file {metadata_file}", exc_info=True
                        )
                        return OrderedDict({})
                    data = xmltodict.parse(for_parsing)
                try:
                    metadata: OrderedDict = OrderedDict({})
                    metadata["experiment_type"] = "tomography"
                    metadata["voltage"] = 300
                    metadata["image_size_x"] = data["Acquisition"]["Info"]["ImageSize"][
                        "Width"
                    ]
                    metadata["image_size_y"] = data["Acquisition"]["Info"]["ImageSize"][
                        "Height"
                    ]
                    metadata["pixel_size_on_image"] = float(
                        data["Acquisition"]["Info"]["SensorPixelSize"]["Height"]
                    )
                    metadata["motion_corr_binning"] = 1
                    metadata["gain_ref"] = None
                    metadata["dose_per_frame"] = (
                        environment.dose_per_frame if environment else None
                    )
                    metadata["source"] = str(self._basepath)
                except KeyError:
                    return OrderedDict({})
                return metadata
            with open(metadata_file, "r") as md:
                mdoc_data = get_global_data(md)
                num_blocks = get_num_blocks(md)
                md.seek(0)
                blocks = [get_block(md) for i in range(num_blocks)]
                mdoc_data_block = blocks[0]
            if not mdoc_data:
                return OrderedDict({})
            mdoc_metadata: OrderedDict = OrderedDict({})
            mdoc_metadata["experiment_type"] = "tomography"
            mdoc_metadata["voltage"] = float(mdoc_data["Voltage"])
            mdoc_metadata["frame_count"] = int(mdoc_data_block["NumSubFrames"])
            mdoc_metadata["tilt_axis"] = float(mdoc_data_block["RotationAngle"])
            mdoc_metadata["image_size_x"] = int(mdoc_data["ImageSize"][0])
            mdoc_metadata["image_size_y"] = int(mdoc_data["ImageSize"][1])
            mdoc_metadata["magnification"] = int(mdoc_data_block["Magnification"])
            superres_binning = int(mdoc_data_block["Binning"])
            binning_factor = 1
            if environment:
                server_config = requests.get(
                    f"{str(environment.url.geturl())}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=environment.instrument_name)}"
                ).json()
                if (
                    server_config.get("superres")
                    and superres_binning == 1
                    and environment.superres
                ):
                    binning_factor = 2
                ps_from_mag = (
                    server_config.get("calibrations", {})
                    .get("magnification", {})
                    .get(mdoc_data_block["Magnification"])
                )
                if ps_from_mag:
                    mdoc_metadata["pixel_size_on_image"] = (
                        float(ps_from_mag) * 1e-10 / binning_factor
                    )
            if mdoc_metadata.get("pixel_size_on_image") is None:
                mdoc_metadata["pixel_size_on_image"] = (
                    float(mdoc_data["PixelSpacing"]) * 1e-10
                )
            mdoc_metadata["motion_corr_binning"] = binning_factor
            mdoc_metadata["gain_ref"] = environment.gain_ref if environment else None
            mdoc_metadata["dose_per_frame"] = (
                environment.dose_per_frame if environment else None
            )
            mdoc_metadata["source"] = str(self._basepath)
            mdoc_metadata["tag"] = str(self._basepath)
            mdoc_metadata["tilt_series_tag"] = metadata_file.stem
            mdoc_metadata["exposure_time"] = float(mdoc_data_block["ExposureTime"])
            slit_width = mdoc_data_block["FilterSlitAndLoss"][0]
            if slit_width.lower() != "nan":
                mdoc_metadata["slit_width"] = float(slit_width)
            mdoc_metadata["file_extension"] = (
                f".{mdoc_data_block['SubFramePath'].split('.')[-1]}"
            )
            mdoc_metadata["eer_fractionation"] = (
                environment.eer_fractionation if environment else None
            ) or 20

            data_file = mdoc_data_block["SubFramePath"].split("\\")[-1]
            if data_file.split(".")[-1] == "eer":
                mdoc_metadata["num_eer_frames"] = murfey.util.eer.num_frames(
                    metadata_file.parent / data_file
                )
                mdoc_metadata["frame_count"] = int(
                    int(mdoc_metadata["eer_fractionation"])
                    / int(mdoc_metadata["num_eer_frames"])
                )
        except Exception as e:
            logger.error(
                f"Exception encountered in metadata gathering: {str(e)}", exc_info=True
            )
            return OrderedDict({})

        return mdoc_metadata
