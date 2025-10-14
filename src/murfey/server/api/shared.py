import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession
from werkzeug.utils import secure_filename

import murfey.server.prometheus as prom
from murfey.util import safe_run, sanitise
from murfey.util.config import MachineConfig, from_file, get_machine_config, settings
from murfey.util.db import (
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    ProcessingJob,
    RsyncInstance,
    Session as MurfeySession,
)

logger = logging.getLogger("murfey.server.api.shared")


@lru_cache(maxsize=5)
def get_machine_config_for_instrument(instrument_name: str) -> Optional[MachineConfig]:
    if settings.murfey_machine_configuration:
        return from_file(Path(settings.murfey_machine_configuration), instrument_name)[
            instrument_name
        ]
    return None


def remove_session_by_id(session_id: int, db):
    session = db.exec(select(MurfeySession).where(MurfeySession.id == session_id)).one()
    sessions_for_visit = db.exec(
        select(MurfeySession).where(MurfeySession.visit == session.visit)
    ).all()
    # Don't remove prometheus metrics if there are other sessions using them
    if len(sessions_for_visit) == 1:
        safe_run(
            prom.monitoring_switch.remove,
            args=(session.visit,),
            label="monitoring_switch",
        )
        rsync_instances = db.exec(
            select(RsyncInstance).where(RsyncInstance.session_id == session_id)
        ).all()
        for ri in rsync_instances:
            safe_run(
                prom.seen_files.remove,
                args=(ri.source, session.visit),
                label="seen_files",
            )
            safe_run(
                prom.transferred_files.remove,
                args=(ri.source, session.visit),
                label="transferred_files",
            )
            safe_run(
                prom.transferred_files_bytes.remove,
                args=(ri.source, session.visit),
                label="transferred_files_bytes",
            )
            safe_run(
                prom.seen_data_files.remove,
                args=(ri.source, session.visit),
                label="seen_data_files",
            )
            safe_run(
                prom.transferred_data_files.remove,
                args=(ri.source, session.visit),
                label="transferred_data_files",
            )
            safe_run(
                prom.transferred_data_files_bytes.remove,
                args=(ri.source, session.visit),
                label="transferred_data_file_bytes",
            )
            safe_run(
                prom.skipped_files.remove,
                args=(ri.source, session.visit),
                label="skipped_files",
            )
    collected_ids = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
    ).all()
    for c in collected_ids:
        safe_run(
            prom.preprocessed_movies.remove,
            args=(c[2].id,),
            label="preprocessed_movies",
        )
    db.delete(session)
    db.commit()
    logger.debug(f"Successfully removed session {session_id} from database")
    return


def get_grid_squares(session_id: int, db):
    grid_squares = db.exec(
        select(GridSquare).where(GridSquare.session_id == session_id)
    ).all()
    tags = {gs.tag for gs in grid_squares}
    res = {}
    for t in tags:
        res[t] = [gs for gs in grid_squares if gs.tag == t]
    return res


def get_grid_squares_from_dcg(session_id: int, dcgid: int, db) -> List[GridSquare]:
    grid_squares = db.exec(
        select(GridSquare, DataCollectionGroup)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
    ).all()
    return [gs[0] for gs in grid_squares]


def get_foil_holes_from_grid_square(
    session_id: int, dcgid: int, gsid: int, db
) -> List[FoilHole]:
    foil_holes = db.exec(
        select(FoilHole, GridSquare, DataCollectionGroup)
        .where(FoilHole.grid_square_id == GridSquare.id)
        .where(GridSquare.name == gsid)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
    ).all()
    return [fh[0] for fh in foil_holes]


def get_foil_hole(session_id: int, fh_name: int, db) -> Dict[str, int]:
    foil_holes = db.exec(
        select(FoilHole, GridSquare)
        .where(FoilHole.name == fh_name)
        .where(FoilHole.session_id == session_id)
        .where(GridSquare.id == FoilHole.grid_square_id)
    ).all()
    return {f[1].tag: f[0].id for f in foil_holes}


def find_upstream_visits(session_id: int, db: SQLModelSession):
    murfey_session = db.exec(
        select(MurfeySession).where(MurfeySession.id == session_id)
    ).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    upstream_visits = {}
    # Iterates through provided upstream directories
    for p in machine_config.upstream_data_directories:
        # Looks for visit name in file path
        for v in Path(p).glob(f"{visit_name.split('-')[0]}-*"):
            upstream_visits[v.name] = v / machine_config.processed_directory_name
    return upstream_visits


def get_upstream_tiff_dirs(visit_name: str, instrument_name: str) -> List[Path]:
    tiff_dirs = []
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    for directory_name in machine_config.upstream_data_tiff_locations:
        for p in machine_config.upstream_data_directories:
            if (Path(p) / secure_filename(visit_name)).is_dir():
                processed_dir = Path(p) / secure_filename(visit_name) / directory_name
                tiff_dirs.append(processed_dir)
                break
    if not tiff_dirs:
        logger.warning(
            f"No candidate directory found for upstream download from visit {sanitise(visit_name)}"
        )
    return tiff_dirs


def gather_upstream_tiffs(visit_name: str, session_id: int, db: SQLModelSession):
    """
    Looks for TIFF files associated with the current session in the permitted storage
    servers, and returns their relative file paths as a list.
    """
    instrument_name = (
        db.exec(select(MurfeySession).where(MurfeySession.id == session_id))
        .one()
        .instrument_name
    )
    upstream_tiff_paths = []
    tiff_dirs = get_upstream_tiff_dirs(visit_name, instrument_name)
    if not tiff_dirs:
        return None
    for tiff_dir in tiff_dirs:
        for f in tiff_dir.glob("**/*.tiff"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
        for f in tiff_dir.glob("**/*.tif"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
    return upstream_tiff_paths


def get_tiff_file(
    visit_name: str, session_id: int, tiff_path: str, db: SQLModelSession
):
    instrument_name = (
        db.exec(select(MurfeySession).where(MurfeySession.id == session_id))
        .one()
        .instrument_name
    )
    tiff_dirs = get_upstream_tiff_dirs(visit_name, instrument_name)
    if not tiff_dirs:
        return None

    tiff_path = "/".join(secure_filename(p) for p in tiff_path.split("/"))
    for tiff_dir in tiff_dirs:
        tiff_file = tiff_dir / tiff_path
        if tiff_file.is_file():
            break
    else:
        logger.warning(f"TIFF {tiff_path} not found")
        return None

    return tiff_file
