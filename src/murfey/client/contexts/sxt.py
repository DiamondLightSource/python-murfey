import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from olefile import OleFileIO

from murfey.client.context import (
    Context,
    _file_transferred_to,
    _get_source,
    ensure_dcg_exists,
)
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post
from murfey.util.models import File
from murfey.util.tomo import midpoint

logger = logging.getLogger("murfey.client.contexts.sxt")


def _find_reference(txrm_file: Path) -> Path | None:
    """Find a suitable reference to apply to the given txrm file"""
    # Look for xrm files in the txrm folder, reverse sorted by time
    candidates = []
    for gf in txrm_file.parent.glob("*.xrm"):
        candidates.append(
            File(
                name=gf.name,
                description="",
                size=gf.stat().st_size / 1e6,
                timestamp=datetime.fromtimestamp(gf.stat().st_mtime),
                full_path=str(gf),
            )
        )
    candidates.sort(key=lambda x: x.timestamp, reverse=True)
    for ref_option in candidates:
        mosaic_size = 1
        with OleFileIO(ref_option.full_path) as xrm_ole:
            # Find images which are not mosaics (txrm spec typos this as mosiac)
            if xrm_ole.exists("ImageInfo/MosiacRows") and xrm_ole.exists(
                "ImageInfo/MosiacColumns"
            ):
                mosaic_size = int(
                    np.frombuffer(
                        xrm_ole.openstream("ImageInfo/MosiacRows").getvalue(), np.int32
                    )[0]
                    * np.frombuffer(
                        xrm_ole.openstream("ImageInfo/MosiacColumns").getvalue(),
                        np.int32,
                    )[0]
                )
        if mosaic_size == 0:
            logger.info(f"Found reference {ref_option.name}")
            return Path(ref_option.full_path)
    logger.warning(f"No reference found for {txrm_file}")
    return None


def _get_ole_header_value(ole_file, title: str, dtype: np.dtype):
    return np.frombuffer(ole_file.openstream(title).getvalue(), dtype)


class SXTContext(Context):
    def __init__(
        self,
        acquisition_software: str,
        basepath: Path,
        machine_config: dict,
        token: str,
    ):
        super().__init__("SXTContext", acquisition_software, token)
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
            ensure_dcg_exists(
                collection_type="sxt",
                metadata_source=self._basepath,
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
                    round(data_collection_parameters.get("pixel_size", 100), 2) * 1e-10
                ),  # expected in metres
                "image_size_x": data_collection_parameters.get("image_size_x", 0),
                "image_size_y": data_collection_parameters.get("image_size_y", 0),
                "magnification": data_collection_parameters.get("magnification", 0),
                "energy": data_collection_parameters.get("energy"),
                "voltage": 0,
                "axis_start": data_collection_parameters.get("minimum_angle"),
                "axis_end": data_collection_parameters.get("maximum_angle"),
                "tilt_series_length": data_collection_parameters.get(
                    "tilt_series_length"
                ),
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

            recipes_to_assign_pjids = self._machine_config.get("recipes", {}).values()
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
        metadata: dict[str, Any] = {}

        if transferred_file.suffix == ".xrm" and environment:
            # Make sure we have a dcg for this grid
            dcg_tag = ensure_dcg_exists(
                collection_type="sxt",
                metadata_source=self._basepath,
                environment=environment,
                machine_config=self._machine_config,
                token=self._token,
            )

            with OleFileIO(str(transferred_file)) as xrm_ole:
                if xrm_ole.exists("ImageInfo/XPosition") and xrm_ole.exists(
                    "ImageInfo/YPosition"
                ):
                    x_tiles = _get_ole_header_value(
                        xrm_ole, "ImageInfo/XPosition", np.float32
                    ).tolist()
                    y_tiles = _get_ole_header_value(
                        xrm_ole, "ImageInfo/YPosition", np.float32
                    ).tolist()
                    metadata["x_position"] = x_tiles[int(len(x_tiles) / 2)]
                    metadata["y_position"] = y_tiles[int(len(y_tiles) / 2)]

                if xrm_ole.exists("ImageInfo/PixelSize"):
                    metadata["pixel_size"] = _get_ole_header_value(
                        xrm_ole, "ImageInfo/PixelSize", np.float32
                    ).tolist()[0]

                if xrm_ole.exists("ImageInfo/ImageHeight"):
                    metadata["height"] = _get_ole_header_value(
                        xrm_ole, "ImageInfo/ImageHeight", np.int32
                    ).tolist()[0]

                if xrm_ole.exists("ImageInfo/ImageWidth"):
                    metadata["width"] = _get_ole_header_value(
                        xrm_ole, "ImageInfo/ImageWidth", np.int32
                    ).tolist()[0]

                # Find images which are not mosaics (txrm spec typos this as mosiac)
                if xrm_ole.exists("ImageInfo/MosiacRows") and xrm_ole.exists(
                    "ImageInfo/MosiacColumns"
                ):
                    metadata["mosaic_rows"] = _get_ole_header_value(
                        xrm_ole, "ImageInfo/MosiacRows", np.int32
                    )[0]
                    metadata["mosaic_columns"] = _get_ole_header_value(
                        xrm_ole, "ImageInfo/MosiacColumns", np.int32
                    )[0]
                    metadata["mosaic_size"] = int(
                        metadata["mosaic_rows"] * metadata["mosaic_columns"]
                    )

            source = _get_source(transferred_file, environment=environment)
            if source:
                image_path = _file_transferred_to(
                    environment,
                    source,
                    transferred_file,
                    Path(self._machine_config.get("rsync_basepath", "")),
                )
                if (
                    environment.visit
                    in Path(environment.default_destinations[source]).parts
                ):
                    # Split either side of the raw directory
                    visit_idx = Path(
                        environment.default_destinations[source]
                    ).parts.index(environment.visit)
                    destination_base = "/".join(
                        Path(environment.default_destinations[source]).parts[
                            : visit_idx + 1
                        ]
                    )
                    destination_extra = "/".join(
                        Path(environment.default_destinations[source]).parts[
                            visit_idx + 2 :
                        ]
                    )
                else:
                    destination_base = str(
                        Path(environment.default_destinations[source])
                        / environment.visit
                    )
                    destination_extra = ""
                converted_file_path = (
                    Path(self._machine_config.get("rsync_basepath", ""))
                    / destination_base
                    / self._machine_config.get("processed_directory_name", "")
                    / self._machine_config.get("processed_extra_directory", "")
                    / destination_extra
                    / f"{transferred_file.relative_to(source).stem}_Annotated.tiff"
                )
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="workflow_sxt.router",
                    function_name="convert_xrm_to_tiff",
                    token=self._token,
                    instrument_name=environment.instrument_name,
                    data={
                        "xrm_path": str(image_path),
                        "tiff_path": str(converted_file_path),
                    },
                )

                if (
                    metadata.get("mosaic_size", 1) > 0
                    and metadata.get("pixel_size", 0) > 0.1
                ):
                    # Large pixel size, this is an atlas
                    dcg_data = {
                        "experiment_type_id": 44,  # Atlas
                        "tag": dcg_tag,
                        "atlas": str(converted_file_path),
                        "atlas_pixel_size": round(metadata.get("pixel_size", 0), 2),
                        "atlas_x_stage_position": metadata.get("x_position", None),
                        "atlas_y_stage_position": metadata.get("y_position", None),
                        "atlas_height": int(
                            metadata.get("height", 0) * metadata["mosaic_rows"]
                        ),
                        "atlas_width": int(
                            metadata.get("width", 0) * metadata["mosaic_columns"]
                        ),
                    }
                    capture_post(
                        base_url=str(environment.url.geturl()),
                        router_name="workflow.router",
                        function_name="register_dc_group",
                        token=self._token,
                        instrument_name=environment.instrument_name,
                        visit_name=environment.visit,
                        session_id=environment.murfey_session,
                        data=dcg_data,
                    )
                elif metadata.get("mosaic_size", 1) > 0:
                    # Other mosaic images are of grid squares
                    capture_post(
                        base_url=str(environment.url.geturl()),
                        router_name="workflow_sxt.router",
                        function_name="register_sxt_roi",
                        token=self._token,
                        instrument_name=environment.instrument_name,
                        visit_name=environment.visit,
                        session_id=environment.murfey_session,
                        data={
                            "tag": dcg_tag,
                            "name": transferred_file.stem,
                            "x_stage_position": metadata.get("x_position", None),
                            "y_stage_position": metadata.get("y_position", None),
                            "pixel_size": round(metadata.get("pixel_size", 0), 2),
                            "height": int(
                                metadata.get("height", 0) * metadata["mosaic_rows"]
                            ),
                            "width": int(
                                metadata.get("width", 0) * metadata["mosaic_columns"]
                            ),
                            "image": str(converted_file_path),
                        },
                    )

        elif transferred_file.suffix == ".txrm" and environment:
            source = _get_source(transferred_file, environment)
            if not source:
                logger.warning(f"No source found for file {transferred_file}")
                return False

            # Read the tilt angles and pixel size from the txrm
            angles: list = []
            metadata["source"] = str(self._basepath)
            metadata["tilt_series_tag"] = transferred_file.stem
            with OleFileIO(str(transferred_file)) as txrm_ole:
                if txrm_ole.exists("ReferenceData/Image"):
                    metadata["has_reference"] = True

                if txrm_ole.exists("ImageInfo/Angles"):
                    angles = _get_ole_header_value(
                        txrm_ole, "ImageInfo/Angles", np.float32
                    ).tolist()
                    metadata["minimum_angle"] = min(angles)
                    metadata["maximum_angle"] = max(angles)

                if txrm_ole.exists("ImageInfo/PixelSize"):
                    pixel_size_txrm = _get_ole_header_value(
                        txrm_ole, "ImageInfo/PixelSize", np.float32
                    ).tolist()
                    metadata["pixel_size"] = pixel_size_txrm[0] * 1e4

                if txrm_ole.exists("ImageInfo/ImageWidth"):
                    image_width_txrm = _get_ole_header_value(
                        txrm_ole, "ImageInfo/ImageWidth", np.int32
                    ).tolist()
                    metadata["image_size_x"] = image_width_txrm[0]

                if txrm_ole.exists("ImageInfo/ImageHeight"):
                    image_height_txrm = _get_ole_header_value(
                        txrm_ole, "ImageInfo/ImageHeight", np.int32
                    ).tolist()
                    metadata["image_size_y"] = image_height_txrm[0]

                if txrm_ole.exists("ImageInfo/ExpTimes"):
                    exposure_time_txrm = _get_ole_header_value(
                        txrm_ole, "ImageInfo/ExpTimes", np.float32
                    ).tolist()
                    metadata["exposure_time"] = exposure_time_txrm[0]

                if txrm_ole.exists("ImageInfo/XrayMagnification"):
                    magnification_txrm = _get_ole_header_value(
                        txrm_ole, "ImageInfo/XrayMagnification", np.float32
                    ).tolist()
                    metadata["magnification"] = magnification_txrm[0]

                if txrm_ole.exists("ImageInfo/ImagesTaken"):
                    tilt_count_txrm = _get_ole_header_value(
                        txrm_ole, "ImageInfo/ImagesTaken", np.int32
                    ).tolist()
                    metadata["tilt_series_length"] = tilt_count_txrm[0]

                if txrm_ole.exists("PositionInfo/AxisNames") and txrm_ole.exists(
                    "PositionInfo/MotorPositions"
                ):
                    # The ImageInfo/Energy field is empty
                    # Instead it needs extracting from the PositionInfo list
                    axis_names = [
                        i
                        for i in txrm_ole.openstream("PositionInfo/AxisNames")
                        .read()
                        .decode("ascii")
                        .split("\x00")
                        if i
                    ]
                    axis_values = _get_ole_header_value(
                        txrm_ole, "PositionInfo/MotorPositions", np.float32
                    )
                    if "Energy" in axis_names:
                        energy_index = list(np.array(axis_names) == "Energy").index(
                            True
                        )
                        metadata["energy"] = int(round(axis_values[energy_index]))

            if (
                not metadata.get("has_reference", False)
                and metadata.get("tilt_series_length", len(angles)) < 20
            ):
                # References are collected with only 10 frames
                logger.debug(f"Reference image {transferred_file} not processed")
                return True
            elif not metadata.get("has_reference", False):
                reference_file = _find_reference(transferred_file)
            else:
                reference_file = None

            if "@" in transferred_file.stem:
                tilt_series_tag = "_".join(
                    transferred_file.stem.split("@")[0].split("_")[:-1]
                )
            else:
                tilt_series_tag = transferred_file.stem
            visit_index = transferred_file.parent.parts.index(environment.visit)
            destination_search_dir = "/".join(
                transferred_file.parts[: visit_index + 2]
            ).replace("//", "/")
            self.register_sxt_data_collection(
                tilt_series=tilt_series_tag,
                data_collection_parameters=metadata,
                file_extension=transferred_file.suffix,
                image_directory=str(
                    Path(
                        environment.default_destinations.get(
                            Path(destination_search_dir), destination_search_dir
                        )
                    )
                    / transferred_file.parent.relative_to(destination_search_dir)
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
            if reference_file:
                reference_file_transferred_to = _file_transferred_to(
                    environment,
                    source,
                    reference_file,
                    Path(self._machine_config.get("rsync_basepath", "")),
                )
            else:
                reference_file_transferred_to = None
            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="workflow_sxt.router",
                function_name="process_sxt_tilt_series",
                token=self._token,
                instrument_name=environment.instrument_name,
                visit_name=environment.visit,
                session_id=environment.murfey_session,
                data={
                    "tag": tilt_series_tag,
                    "source": destination_search_dir,
                    "pixel_size": round(
                        metadata.get("pixel_size", 100), 2
                    ),  # angstroms
                    "tilt_offset": midpoint(angles),
                    "tilt_series_length": metadata.get(
                        "tilt_series_length", len(angles)
                    ),
                    "txrm": str(file_transferred_to),
                    "xrm_reference": str(reference_file_transferred_to)
                    if reference_file_transferred_to
                    else None,
                },
            )
        return True
