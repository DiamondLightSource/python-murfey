"""
Functions shared by the FIB workflows
"""

import numpy as np

from murfey.util.db import ImagingSite
from murfey.util.models import FIBImageMetadata


def populate_fib_imaging_site_entry(
    imaging_site: ImagingSite,
    metadata: FIBImageMetadata,
):
    imaging_site.image_path = str(metadata.file)
    imaging_site.pos_x = metadata.pos_x
    imaging_site.pos_y = metadata.pos_y
    imaging_site.pos_z = metadata.pos_z
    imaging_site.rotation = float(np.rad2deg(metadata.rotation))
    imaging_site.tilt_alpha = float(np.rad2deg(metadata.tilt_alpha))
    imaging_site.tilt_beta = float(np.rad2deg(metadata.tilt_beta))
    imaging_site.len_x = metadata.len_x
    imaging_site.len_y = metadata.len_y
    imaging_site.image_pixels_x = metadata.pixels_x
    imaging_site.image_pixels_y = metadata.pixels_y
    imaging_site.image_pixel_size = metadata.pixel_size

    if metadata.thumbnail_path is not None:
        scale = 512 / (max(metadata.pixels_x, metadata.pixels_y) or 1)
        imaging_site.thumbnail_path = str(metadata.thumbnail_path)
        imaging_site.thumbnail_pixels_x = int(round(metadata.pixels_x * scale)) or 1
        imaging_site.thumbnail_pixels_y = int(round(metadata.pixels_y * scale)) or 1
        imaging_site.thumbnail_pixel_size = metadata.pixel_size / scale

    return imaging_site
