from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, OrderedDict

import requests
import xmltodict

from murfey.client.context import Context, ProcessingParameter
from murfey.client.instance_environment import (
    MovieID,
    MovieTracker,
    MurfeyID,
    MurfeyInstanceEnvironment,
)
from murfey.util import get_machine_config

logger = logging.getLogger("murfey.client.contexts.spa")


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
        ProcessingParameter("eer_grouping", "EER Grouping", default=20),
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
        super().__init__(acquisition_software)
        self._basepath = basepath
        self._processing_job_stash: dict = {}
        self._preprocessing_triggers: dict = {}

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
            num_fractions = int(
                data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"][
                    "CameraSpecificInput"
                ]["a:KeyValueOfstringanyType"][2]["a:Value"]["b:NumberOffractions"]
            )
            metadata["c2aperture"] = data["MicroscopeImage"]["CustomData"][
                "a:KeyValueOfstringanyType"
            ][3]["a:Value"]["#text"]
            metadata["exposure_time"] = data["MicroscopeImage"]["microscopeData"][
                "acquisition"
            ]["camera"]["ExposureTime"]
            metadata["slit_width"] = data["MicroscopeImage"]["microscopeData"][
                "optics"
            ]["EnergyFilter"]["EnergySelectionSlitWidth"]
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
        binning_factor = int(
            data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"][
                "Binning"
            ]["a:x"]
        )
        if binning_factor == 2:
            metadata["image_size_x"] = str(
                int(metadata["image_size_x"]) * binning_factor
            )
            metadata["image_size_y"] = str(
                int(metadata["image_size_y"]) * binning_factor
            )
        if environment:
            server_config = requests.get(
                f"{str(environment.url.geturl())}/machine/"
            ).json()
            if server_config.get("superres") and environment.superres:
                binning_factor = 2
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
                        metadata["pixel_size_on_image"] *= binning_factor
        metadata["pixel_size_on_image"] = (
            metadata["pixel_size_on_image"] / binning_factor
        )
        metadata["motion_corr_binning"] = binning_factor
        metadata["gain_ref"] = (
            f"data/{datetime.now().year}/{environment.visit}/processing/gain.mrc"
            if environment
            else None
        )
        metadata["gain_ref_superres"] = (
            f"data/{datetime.now().year}/{environment.visit}/processing/gain_superres.mrc"
            if environment
            else None
        )
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
        metadata["eer_grouping"] = (
            environment.data_collection_parameters.get("eer_grouping")
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
    def post_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
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
                    required_strings = machine_config.get(
                        "data_required_substrings", {}
                    ).get("epu", ["fractions"])

                    if not environment:
                        logger.warning("No environment passed in")
                        return
                    source = _get_source(transferred_file, environment)
                    if not source:
                        logger.warning(f"No source found for file {transferred_file}")
                        return

                    for r in required_strings:
                        if r not in transferred_file.name.lower():
                            return

                    if environment:
                        file_transferred_to = _file_transferred_to(
                            environment, source, transferred_file
                        )
                        environment.movies[file_transferred_to] = MovieTracker(
                            movie_number=next(MovieID),
                            motion_correction_uuid=next(MurfeyID),
                        )

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
                            "downscale": environment.data_collection_parameters.get(
                                "downscale"
                            ),
                            "tag": str(source),
                        }
                        requests.post(
                            preproc_url,
                            json={
                                k: None if v == "None" else v
                                for k, v in preproc_data.items()
                            },
                        )

        return

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
        requests.post(url, json=json)

    def post_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        return

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
                "eer_grouping": parameters["eer_grouping"],
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
        requests.post(proc_url, json=msg)

    def _launch_spa_pipeline(
        self,
        tag: str,
        jobid: int,
        environment: MurfeyInstanceEnvironment,
        url: str = "",
    ):
        environment.id_tag_registry["auto_proc_program"].append(tag)
        data = {"job_id": jobid}
        requests.post(url, json=data)
