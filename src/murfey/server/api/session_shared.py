import logging
from pathlib import Path
from typing import Dict, List

from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession
from werkzeug.utils import secure_filename

import murfey.server.prometheus as prom
from murfey.util import safe_run, sanitise, secure_path
from murfey.util.config import get_machine_config
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
    """
    Returns a nested dictionary, in which visits and the full paths to their directories
    are further grouped by instrument name.
    """
    murfey_session = db.exec(
        select(MurfeySession).where(MurfeySession.id == session_id)
    ).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    upstream_visits: dict[str, dict[str, Path]] = {}
    # Iterates through provided upstream directories
    for (
        upstream_instrument,
        upstream_data_dir,
    ) in machine_config.upstream_data_directories.items():
        # Looks for visit name in file path
        current_upstream_visits = {}
        for visit_path in Path(upstream_data_dir).glob(f"{visit_name.split('-')[0]}-*"):
            if visit_path.is_dir():
                current_upstream_visits[visit_path.name] = visit_path
        upstream_visits[upstream_instrument] = current_upstream_visits
    return upstream_visits


def gather_upstream_files(
    session_id: int,
    upstream_instrument: str,
    upstream_visit_path: Path,
    db: SQLModelSession,
):
    """
    Searches the specified upstream instrument for files based on the search strings
    set in the MachineConfig and returns them as a list of file paths.
    """
    # Load the current instrument's machine config
    murfey_session = db.exec(
        select(MurfeySession).where(MurfeySession.id == session_id)
    ).one()
    instrument_name = murfey_session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]

    # Search for files using the configured strings for that upstream instrument
    file_list: list[Path] = []
    logger.info(f"Searching for files in {sanitise(str(upstream_visit_path))!r}")
    if (
        machine_config.upstream_data_search_strings.get(upstream_instrument, None)
        is not None
    ):
        for search_string in machine_config.upstream_data_search_strings[
            upstream_instrument
        ]:
            logger.info(f"Using search string {search_string}")
            for file in upstream_visit_path.glob(search_string):
                if file.is_file():
                    file_list.append(file)
        logger.info(
            f"Found {len(file_list)} files for download "
            f"from {sanitise(upstream_instrument)}"
        )
    else:
        logger.warning(
            "Upstream file searching has not been configured for "
            f"{sanitise(upstream_instrument)} on {sanitise(instrument_name)}"
        )
    return file_list


def get_upstream_file(file_path: str | Path):
    file_path = Path(file_path) if isinstance(file_path, str) else file_path
    file_path = secure_path(file_path)
    if file_path.exists() and file_path.is_file():
        return file_path
    logger.warning(f"Requested file {sanitise(str(file_path))!r} was not found")
    return None


def get_upstream_tiff_dirs(visit_name: str, instrument_name: str) -> List[Path]:
    tiff_dirs = []
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    for directory_name in machine_config.upstream_data_tiff_locations:
        for _, p in machine_config.upstream_data_directories.items():
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
