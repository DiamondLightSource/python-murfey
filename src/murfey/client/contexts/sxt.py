import logging
from pathlib import Path
from typing import Any

from txrm2tiff.inspector import Inspector
from txrm2tiff.txrm import open_txrm
from txrm2tiff.txrm_functions.general import read_stream
from txrm2tiff.xradia_properties.enums import XrmDataTypes

from murfey.client.context import (
    Context,
    _file_transferred_to,
    _get_source,
    ensure_dcg_exists,
)
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post
from murfey.util.tomo import midpoint

logger = logging.getLogger("murfey.client.contexts.sxt")


class SXTContext(Context):
    def __init__(
        self,
        acquisition_software: str,
        basepath: Path,
        machine_config: dict,
        token: str,
    ):
        super().__init__("SXT", acquisition_software, token)
        self._basepath = basepath
        self._machine_config = machine_config

    def register_sxt_data_collection(
        self,
        tilt_series: str,
        data_collection_parameters: dict,
        file_extension: str,
        image_directory: str | Path,
        environment: MurfeyInstanceEnvironment | None = None,
    ):
        if not environment:
            logger.error(
                "No environment passed to register tomography data collections"
            )
            return
        try:
            metadata_source = (
                self._basepath.parent / environment.visit / self._basepath.name
            )
            ensure_dcg_exists(
                collection_type="sxt",
                metadata_source=metadata_source,
                environment=environment,
                machine_config=self._machine_config,
                token=self._token,
            )

            dc_data: dict[str, Any] = {
                "experiment_type": "sxt",
                "file_extension": file_extension,
                "acquisition_software": self._acquisition_software,
                "image_directory": str(image_directory),
                "data_collection_tag": tilt_series,
                "source": str(self._basepath),
                "tag": tilt_series,
                "pixel_size_on_image": str(
                    data_collection_parameters.get("pixel_size", 100)
                ),
                "image_size_x": data_collection_parameters.get("image_size_x", 0),
                "image_size_y": data_collection_parameters.get("image_size_y", 0),
                "magnification": data_collection_parameters.get("magnification", 0),
                "voltage": 0,
            }
            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="workflow.router",
                function_name="start_dc",
                token=self._token,
                instrument_name=environment.instrument_name,
                visit_name=environment.visit,
                session_id=environment.murfey_session,
                data=dc_data,
            )

            recipes_to_assign_pjids = [
                "sxt-tomo-align",
            ]
            for recipe in recipes_to_assign_pjids:
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="workflow.router",
                    function_name="register_proc",
                    token=self._token,
                    instrument_name=environment.instrument_name,
                    visit_name=environment.visit,
                    session_id=environment.murfey_session,
                    data={
                        "tag": tilt_series,
                        "source": str(self._basepath),
                        "recipe": recipe,
                        "experiment_type": "sxt",
                    },
                )
        except Exception as e:
            logger.error(f"ERROR {e}, {data_collection_parameters}", exc_info=True)

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

        data_suffixes = [".txrm"]

        if transferred_file.suffix in data_suffixes and environment:
            source = _get_source(transferred_file, environment)
            if not source:
                logger.warning(f"No source found for file {transferred_file}")
                return False

            # Read the tilt angles and pixel size from the txrm
            metadata = {
                "source": str(self._basepath),
                "tilt_series_tag": transferred_file.stem,
            }
            with open_txrm(
                transferred_file, load_images=False, load_reference=False, strict=False
            ) as txrm:
                inspector = Inspector(txrm)
                angles = read_stream(
                    inspector.txrm.ole,
                    "ImageInfo/Angles",
                    XrmDataTypes.XRM_FLOAT,
                    strict=True,
                )
                if angles:
                    metadata["minimum_angle"] = min(angles)
                    metadata["maximum_angle"] = max(angles)

                pixel_size_txrm = read_stream(
                    inspector.txrm.ole,
                    "ImageInfo/PixelSize",
                    XrmDataTypes.XRM_FLOAT,
                    strict=True,
                )
                if pixel_size_txrm:
                    metadata["pixel_size"] = pixel_size_txrm[0] * 1e4

                image_width_txrm = read_stream(
                    inspector.txrm.ole,
                    "ImageInfo/ImageWidth",
                    XrmDataTypes.XRM_INT,
                    strict=True,
                )
                if image_width_txrm:
                    metadata["image_size_x"] = image_width_txrm[0]

                image_height_txrm = read_stream(
                    inspector.txrm.ole,
                    "ImageInfo/ImageHeight",
                    XrmDataTypes.XRM_INT,
                    strict=True,
                )
                if image_height_txrm:
                    metadata["image_size_y"] = image_height_txrm[0]

                exposure_time_txrm = read_stream(
                    inspector.txrm.ole,
                    "ImageInfo/ExpTimes",
                    XrmDataTypes.XRM_FLOAT,
                    strict=True,
                )
                if exposure_time_txrm:
                    metadata["exposure_time"] = exposure_time_txrm[0]

                magnification_txrm = read_stream(
                    inspector.txrm.ole,
                    "ImageInfo/XrayMagnification",
                    XrmDataTypes.XRM_FLOAT,
                    strict=True,
                )
                if magnification_txrm:
                    metadata["magnification"] = magnification_txrm[0]

                tilt_count_txrm = read_stream(
                    inspector.txrm.ole,
                    "ImageInfo/ImagesTaken",
                    XrmDataTypes.XRM_INT,
                    strict=True,
                )
                if tilt_count_txrm:
                    metadata["tilt_count"] = tilt_count_txrm[0]

            self.register_sxt_data_collection(
                tilt_series=transferred_file.stem,
                data_collection_parameters=metadata,
                file_extension=transferred_file.suffix,
                image_directory=environment.default_destinations.get(
                    transferred_file.parent, transferred_file.parent
                ),
                environment=environment,
            )

            logger.info(
                f"The following tilt series will be processed: {transferred_file.stem}"
            )
            file_transferred_to = _file_transferred_to(
                environment,
                source,
                transferred_file,
                Path(self._machine_config.get("rsync_basepath", "")),
            )
            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="workflow.sxt_router",
                function_name="process_sxt_tilt_series",
                token=self._token,
                instrument_name=environment.instrument_name,
                visit_name=environment.visit,
                session_id=environment.murfey_session,
                data={
                    "session_id": environment.murfey_session,
                    "tag": transferred_file.stem,
                    "source": str(transferred_file.parent),
                    "pixel_size": metadata.get("pixel_size", 100),
                    "tilt_offset": midpoint(angles),
                    "txrm": str(file_transferred_to),
                },
            )
        return True
