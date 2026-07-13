import logging
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from pipeliner.project_graph import ProjectGraph
from pydantic import BaseModel
from werkzeug.utils import secure_filename

from murfey.util import sanitise, secure_path
from murfey.util.config import MachineConfig, get_machine_config

logger = logging.getLogger("murfey.util.processing_params")


_DEFAULT_MOTIONCORR_FALLBACK = "job002"


@lru_cache(maxsize=16)
def _job_dir_for_alias_cached(visit_name: str, alias: str, mtime_ns: int) -> str | None:
    """Read default_pipeline.star and return the jobNNN for the given alias.

    Returns None on any failure (missing file, graph read error, alias
    not found). The mtime_ns argument is a cache key — when Pipeliner rewrites
    default_pipeline.star its mtime changes and the next call falls through
    to a fresh read.
    """
    project_dir = secure_path(Path(visit_name))
    pipeline_file = project_dir / "default_pipeline.star"
    if not pipeline_file.is_file():
        return None
    try:
        with ProjectGraph(pipeline_dir=project_dir, read_only=True) as graph:
            for proc in graph.process_list:
                proc_alias = getattr(proc, "alias", None)
                if proc_alias and proc_alias.rstrip("/").endswith(alias):
                    # proc.name is e.g. "MotionCorr/job003/"
                    return Path(proc.name).name
    except Exception:
        logger.error(
            f"ProjectGraph read failed while looking up alias {sanitise(str(alias))} "
            f"in {sanitise(str(pipeline_file))}",
            exc_info=True,
        )
        return None
    return None


def _job_dir_for_alias(visit_name: str, alias: str) -> str:
    """Return the Pipeliner jobNNN for alias in the given project.

    visit_name must be an path to the project directory.
    Falls back to the positional default job002 and logs a warning so
    drift from the live pipeline is visible in the logs instead of silent.
    """
    project_dir = secure_path(Path(visit_name)).resolve()
    pipeline_file = project_dir / "default_pipeline.star"
    try:
        mtime_ns = pipeline_file.stat().st_mtime_ns
    except FileNotFoundError:
        logger.warning(
            f"default_pipeline.star missing at {sanitise(str(pipeline_file))} "
            f"— falling back to {sanitise(str(_DEFAULT_MOTIONCORR_FALLBACK))} for alias {sanitise(str(alias))}",
        )
        return _DEFAULT_MOTIONCORR_FALLBACK
    job_dir = _job_dir_for_alias_cached(str(project_dir), alias, mtime_ns)
    if job_dir is None:
        logger.warning(
            f"Alias {sanitise(str(alias))} not found in {sanitise(str(pipeline_file))} "
            f"— falling back to {sanitise(str(_DEFAULT_MOTIONCORR_FALLBACK))}",
        )
        return _DEFAULT_MOTIONCORR_FALLBACK
    return job_dir


def motion_corrected_mrc(
    input_movie: Path, visit_name: str, machine_config: MachineConfig
):
    movie = os.path.basename(input_movie)
    job_dir = _job_dir_for_alias(visit_name, "Live_motioncorr")
    mrc_out = (
        Path(visit_name)
        / "MotionCorr"
        / job_dir
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
    return machine_config.default_model or Path("")


class CLEMProcessingParameters(BaseModel):
    # Atlas vs GridSquare registration threshold
    atlas_threshold: float = 0.0015  # in m


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
