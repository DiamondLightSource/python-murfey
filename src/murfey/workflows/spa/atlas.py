import logging
from pathlib import Path

import mrcfile
import PIL.Image
from werkzeug.utils import secure_filename

from murfey.util import sanitise
from murfey.util.config import get_machine_config

logger = logging.getLogger("murfey.workflows.spa.atlas")


def atlas_jpg_from_mrc(instrument_name: str, visit_name: str, atlas_mrc: Path):
    logger.debug(
        f"Starting workflow to create JPG image of atlas {sanitise(str(atlas_mrc))!r}"
    )
    with mrcfile.open(atlas_mrc) as mrc:
        data = mrc.data

    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]

    parts = [secure_filename(p) for p in atlas_mrc.parts]
    visit_idx = parts.index(visit_name)
    core = Path("/".join(parts[: visit_idx + 1]))
    sample_id = "Sample"
    for p in parts:
        if "Sample" in p:
            sample_id = p
            break
    atlas_jpg_file = (
        core
        / machine_config.processed_directory_name
        / "atlas"
        / secure_filename(f"{sample_id}_{atlas_mrc.stem}_fullres.jpg")
    )
    atlas_jpg_file.parent.mkdir(parents=True, exist_ok=True)

    data = data - data.min()
    data = data.astype(float) * 255 / data.max()
    data = data.astype("uint8")
    PIL.Image.fromarray(data).save(atlas_jpg_file)
    logger.debug(f"JPG image of atlas saved as {str(atlas_jpg_file)!r}")
