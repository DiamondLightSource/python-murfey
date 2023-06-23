from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, OrderedDict

import requests
import xmltodict

from murfey.client.context import Context, ProcessingParameter
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util import get_machine_config

logger = logging.getLogger("murfey.client.contexts.spa")


class SPAContext(Context):
    user_params = [
        ProcessingParameter(
            "dose_per_frame", "Dose Per Frame (e- / Angstrom^2 / frame)"
        ),
        ProcessingParameter(
            "estimate_particle_diameter",
            "Use crYOLO to Estimate Particle Diameter",
            default=True,
        ),
        ProcessingParameter(
            "particle_diameter", "Particle Diameter (Angstroms)", default=0
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
        proc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/register_processing_job"
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
                "motioncor_gainreference": parameters["gain_ref"],
                "motioncor_doseperframe": parameters["dose_per_frame"],
                "eer_grouping": parameters["eer_grouping"],
                "import_images": import_images,
                "angpix": float(parameters["pixel_size_on_image"]) * 1e10,
                "symmetry": parameters["symmetry"],
                "extract_boxsize": parameters["boxsize"],
                "extract_downscale": parameters["downscale"],
                "extract_small_boxsize": parameters["small_boxsize"],
                "mask_diameter": parameters["mask_diameter"],
                "autopick_do_cryolo": parameters["use_cryolo"],
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

    def gather_metadata(
        self, metadata_file: Path, environment: MurfeyInstanceEnvironment | None = None
    ):
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
            metadata["total_exposed_dose"] = data["MicroscopeImage"]["CustomData"][
                "a:KeyValueOfstringanyType"
            ][10]["a:Value"]["#text"] * (
                1e-20
            )  # convert e / m^2 to e / A^2
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
                    .get(int(magnification))
                )
                if ps_from_mag:
                    metadata["pixel_size_on_image"] = float(ps_from_mag) * 1e-10
        metadata["pixel_size_on_image"] = (
            metadata["pixel_size_on_image"] / binning_factor
        )
        metadata["motion_corr_binning"] = binning_factor
        metadata["gain_ref"] = (
            f"data/{datetime.now().year}/{environment.visit}/processing/gain.mrc"
            if environment
            else None
        )
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

        for k, v in metadata.items():
            if v == "None":
                metadata.pop(k)

        return metadata