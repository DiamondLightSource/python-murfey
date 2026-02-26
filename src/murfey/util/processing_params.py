from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel
from werkzeug.utils import secure_filename

from gemmi import cif  
from pipeliner.star_keys import GENERAL_BLOCK, JOB_COUNTER

from murfey.util.config import MachineConfig, get_machine_config

import os

def get_current_job_number(visit_name: str, machine_config: MachineConfig) -> int:
    if os.path.exists(visit_name):
        default_pipeline_path = os.path.join(visit_name, "default_pipeline.star")
    """elif machine_config.processed_directory_name:
        core = Path(visit_name).parts[0]
        extra_path = machine_config.processed_extra_directory
        sub
        default_pipeline_path = (core
        / machine_config.processed_directory_name
        / sub_dataset
        / extra_path)"""
    if os.path.exists(default_pipeline_path):
        dp = cif.read_file(default_pipeline_path)  
        dp_job_counter = dp.find_block(GENERAL_BLOCK).find_value(JOB_COUNTER)  
        current_counter = int(dp_job_counter)
        return current_counter
    
    return 2  # Default to job002 if the file doesn't exist or the value is not found

def motion_corrected_mrc(
    input_movie: Path, visit_name: str, machine_config: MachineConfig
):
    movie = os.path.basename(input_movie)

    """ if not os.path.exists(visit_name):
    parts = [secure_filename(p) for p in input_movie.parts]
    visit_idx = parts.index(visit_name)
    core = Path("/") / Path(*parts[: visit_idx + 1])
    ppath = Path("/") / Path(*parts)
    if machine_config.process_multiple_datasets:
        sub_dataset = ppath.relative_to(core).parts[0]
    else:
        sub_dataset = ""
    extra_path = machine_config.processed_extra_directory
    """

    job_number = get_current_job_number(visit_name, machine_config)

    mrc_out = (
        Path(visit_name)
        / "MotionCorr"
        / f"Live_processing_mc"
        / "Movies"
        / str(movie + "_motion_corrected.mrc")
    )
    return Path("/".join(secure_filename(p) for p in mrc_out.parts))


@lru_cache(maxsize=5)
def cryolo_model_path(visit: str, instrument_name: str) -> Path:
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.picking_model_search_directory:
        visit_directory = (
            (machine_config.rsync_basepath or Path("")).resolve()
            / str(datetime.now().year)
            / visit
        )
        possible_models = list(
            (visit_directory / machine_config.picking_model_search_directory).glob(
                "*.h5"
            )
        )
        if possible_models:
            return sorted(possible_models, key=lambda x: x.stat().st_ctime)[-1]
    return (machine_config.default_model or Path("")).resolve()


class CLEMProcessingParameters(BaseModel):
    # Atlas vs GridSquare registration threshold
    atlas_threshold: float = 0.0015  # in m

    # Image alignment and merging-specific parameters
    crop_to_n_frames: Optional[int] = 50
    align_self: Literal["enabled", ""] = "enabled"
    flatten: Literal["mean", "min", "max", ""] = "mean"
    align_across: Literal["enabled", ""] = "enabled"


default_clem_processing_parameters = CLEMProcessingParameters()


class SPAParameters(BaseModel):
    nr_iter_2d: int = 25
    nr_iter_3d: int = 25
    nr_iter_ini_model: int = 100
    batch_size_2d: int = 50000
    nr_classes_2d: int = 50
    nr_classes_3d: int = 4
    downscale: bool = True
    do_icebreaker_jobs: bool = True
    fraction_of_classes_to_remove_2d: float = 0.7
    nr_picks_before_diameter: int = 10000
    bfactor_min_particles: int = 2000


default_spa_parameters = SPAParameters()


class TomographyParameters(BaseModel):
    batch_size_2d: int = 5000
    nr_classes_2d: int = 5


default_tomo_parameters = TomographyParameters()
