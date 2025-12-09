import logging
from pathlib import Path
from typing import Optional

import xmltodict

from murfey.client.context import Context, _atlas_destination
from murfey.client.contexts.spa import _get_source
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post

logger = logging.getLogger("murfey.client.contexts.atlas")


class AtlasContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path, token: str):
        super().__init__("Atlas", acquisition_software, token)
        self._basepath = basepath

    def post_transfer(
        self,
        transferred_file: Path,
        environment: Optional[MurfeyInstanceEnvironment] = None,
        **kwargs,
    ):
        super().post_transfer(
            transferred_file=transferred_file,
            environment=environment,
            **kwargs,
        )

        if (
            environment
            and "Atlas_" in transferred_file.stem
            and transferred_file.suffix == ".mrc"
        ):
            source = _get_source(transferred_file, environment)
            if source:
                transferred_atlas_name = _atlas_destination(
                    environment, source, self._token
                ) / transferred_file.relative_to(source.parent)
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="session_control.spa_router",
                    function_name="make_atlas_jpg",
                    token=self._token,
                    session_id=environment.murfey_session,
                    data={"path": str(transferred_atlas_name).replace("//", "/")},
                )
                logger.info(
                    f"Submitted request to create JPG image of atlas {str(transferred_atlas_name)!r}"
                )
        elif (
            environment
            and "Atlas_" in transferred_file.stem
            and transferred_file.suffix == ".xml"
        ):
            source = _get_source(transferred_file, environment)
            if source:
                atlas_mrc = transferred_file.with_suffix(".mrc")
                transferred_atlas_jpg = _atlas_destination(
                    environment, source, self._token
                ) / atlas_mrc.relative_to(source.parent).with_suffix(".jpg")

                with open(transferred_file, "rb") as atlas_xml:
                    atlas_xml_data = xmltodict.parse(atlas_xml)
                    atlas_original_pixel_size = float(
                        atlas_xml_data["MicroscopeImage"]["SpatialScale"]["pixelSize"][
                            "x"
                        ]["numericValue"]
                    )

                # need to calculate the pixel size of the downscaled image
                atlas_pixel_size = atlas_original_pixel_size * 7.8

                for p in transferred_file.parts:
                    if p.startswith("Sample"):
                        sample = int(p.replace("Sample", ""))
                        break
                else:
                    logger.warning(
                        f"Sample could not be identified for {transferred_file}"
                    )
                    return

                dcg_data = {
                    "experiment_type_id": 44,  # Atlas
                    "tag": str(transferred_file.parent),
                    "atlas": str(transferred_atlas_jpg).replace("//", "/"),
                    "sample": sample,
                    "atlas_pixel_size": atlas_pixel_size,
                }
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="workflow.router",
                    function_name="register_dc_group",
                    token=self._token,
                    visit_name=environment.visit,
                    session_id=environment.murfey_session,
                    data=dcg_data,
                )
                logger.info(
                    f"Registered data collection group for atlas {str(transferred_atlas_jpg)!r}"
                )
