import logging
from pathlib import Path

from sqlmodel import Session, select

import murfey.server
from murfey.server.feedback import _murfey_id
from murfey.util.config import get_machine_config, get_microscope
from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    Movie,
    PreprocessStash,
    ProcessingJob,
    Session as MurfeySession,
    SPARelionParameters,
)
from murfey.util.models import FoilHoleParameters, GridSquareParameters
from murfey.util.processing_params import cryolo_model_path, default_spa_parameters
from murfey.util.spa_metadata import (
    GridSquareInfo,
    foil_hole_data,
    foil_hole_from_file,
    get_grid_square_atlas_positions,
    grid_square_data,
    grid_square_from_file,
)
from murfey.workflows.spa.register_foil_holes import register_foil_holes
from murfey.workflows.spa.register_grid_square import register_grid_square

logger = logging.getLogger("murfey.workflows.spa.flush_spa_preprocess")


def _grid_square_metadata_file(f: Path, grid_square: int) -> Path | None:
    """Search through metadata directories to find the required grid square dm"""
    raw_dir = f.parent.parent.parent
    metadata_dirs = raw_dir.glob("metadata*")
    gs_path = None
    for md_dir in metadata_dirs:
        gs_path = md_dir / f"Metadata/GridSquare_{grid_square}.dm"
        if gs_path.is_file():
            return gs_path
    logger.error(f"Grid square metadata path {gs_path} does not exist for {f}")
    return gs_path


def _flush_position_analysis(
    movie_path: Path, dcg_id: int, session_id: int, murfey_db: Session
) -> list[int | None] | None:
    """Register a grid square and foil hole in the database"""
    data_collection_group = murfey_db.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.id == dcg_id)
    ).one()

    # Work out the grid square and associated metadata file
    grid_square = grid_square_from_file(movie_path)
    grid_square_metadata_file = _grid_square_metadata_file(movie_path, grid_square)
    if grid_square_metadata_file:
        gs = grid_square_data(grid_square_metadata_file, grid_square)
    else:
        gs = GridSquareInfo(id=grid_square)
    if data_collection_group.atlas:
        # If an atlas if present, work out where this grid square is on it
        gs_pix_position = get_grid_square_atlas_positions(
            data_collection_group.atlas,
            grid_square=str(grid_square),
        )[str(grid_square)]
        grid_square_parameters = GridSquareParameters(
            tag=data_collection_group.tag,
            x_location=gs_pix_position[0],
            y_location=gs_pix_position[1],
            x_stage_position=gs_pix_position[2],
            y_stage_position=gs_pix_position[3],
            readout_area_x=gs.readout_area_x,
            readout_area_y=gs.readout_area_y,
            thumbnail_size_x=gs.thumbnail_size_x,
            thumbnail_size_y=gs.thumbnail_size_y,
            height=gs_pix_position[5],
            width=gs_pix_position[4],
            pixel_size=gs.pixel_size,
            image=gs.image,
            angle=gs_pix_position[6],
        )
    else:
        # Skip location analysis if no atlas
        grid_square_parameters = GridSquareParameters(
            tag=data_collection_group.tag,
            readout_area_x=gs.readout_area_x,
            readout_area_y=gs.readout_area_y,
            thumbnail_size_x=gs.thumbnail_size_x,
            thumbnail_size_y=gs.thumbnail_size_y,
            pixel_size=gs.pixel_size,
            image=gs.image,
        )
    # Insert or update this grid square in the database
    register_grid_square(session_id, gs.id, grid_square_parameters, murfey_db)

    # Find the foil hole info and register it
    foil_hole = foil_hole_from_file(movie_path)
    if grid_square_metadata_file:
        fh = foil_hole_data(
            grid_square_metadata_file,
            foil_hole,
            grid_square,
        )
        foil_hole_parameters = FoilHoleParameters(
            tag=data_collection_group.tag,
            name=foil_hole,
            x_location=fh.x_location,
            y_location=fh.y_location,
            x_stage_position=fh.x_stage_position,
            y_stage_position=fh.y_stage_position,
            readout_area_x=fh.readout_area_x,
            readout_area_y=fh.readout_area_y,
            thumbnail_size_x=fh.thumbnail_size_x,
            thumbnail_size_y=fh.thumbnail_size_y,
            pixel_size=fh.pixel_size,
            image=fh.image,
            diameter=fh.diameter,
        )
    else:
        foil_hole_parameters = FoilHoleParameters(
            tag=data_collection_group.tag,
            name=foil_hole,
        )
    # Insert or update this foil hole in the database
    return register_foil_holes(
        session_id, gs.id, {foil_hole: foil_hole_parameters}, murfey_db
    )


def flush_spa_preprocess(message: dict, murfey_db: Session) -> dict[str, bool]:
    session_id = int(message["session_id"])
    stashed_files = murfey_db.exec(
        select(PreprocessStash)
        .where(PreprocessStash.session_id == session_id)
        .where(PreprocessStash.tag == message["tag"])
    ).all()
    if not stashed_files:
        return {"success": True}

    murfey_session = murfey_db.exec(
        select(MurfeySession).where(MurfeySession.id == session_id)
    ).one()
    machine_config = get_machine_config(instrument_name=murfey_session.instrument_name)[
        murfey_session.instrument_name
    ]
    recipe_name = machine_config.recipes.get("em-spa-preprocess", "em-spa-preprocess")
    collected_ids = murfey_db.exec(
        select(
            DataCollectionGroup,
            DataCollection,
            ProcessingJob,
            AutoProcProgram,
        )
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == message["tag"])
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == recipe_name)
    ).one()
    proc_params = murfey_db.exec(
        select(SPARelionParameters).where(
            SPARelionParameters.pj_id == collected_ids[2].id
        )
    ).one()

    murfey_ids = _murfey_id(
        collected_ids[3].id,
        murfey_db,
        number=2 * len(stashed_files),
        close=False,
    )
    for i, f in enumerate(stashed_files):
        try:
            foil_hole_id = None
            if f.foil_hole_id:
                # Check if the foil hole id has been registered in the database
                db_foil_hole = murfey_db.exec(
                    select(FoilHole).where(FoilHole.id == f.foil_hole_id)
                ).all()
                if db_foil_hole:
                    foil_hole_id = f.foil_hole_id
            if not foil_hole_id:
                # Register grid square and foil hole if not present
                foil_hole_id_list = _flush_position_analysis(
                    movie_path=Path(f.file_path),
                    dcg_id=collected_ids[0].id,
                    session_id=session_id,
                    murfey_db=murfey_db,
                )
                if foil_hole_id_list:
                    foil_hole_id = foil_hole_id_list[0]
        except Exception as e:
            logger.error(
                f"Flushing position analysis for {f.file_path} caused exception {e}",
                exc_info=True,
            )
            foil_hole_id = None

        ppath = Path(f.file_path)
        movie = Movie(
            murfey_id=murfey_ids[2 * i],
            data_collection_id=collected_ids[1].id,
            path=f.file_path,
            image_number=f.image_number,
            tag=f.tag,
            foil_hole_id=foil_hole_id,
        )
        murfey_db.add(movie)
        zocalo_message: dict = {
            "recipes": [recipe_name],
            "parameters": {
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": collected_ids[1].id,
                "kv": proc_params.voltage,
                "autoproc_program_id": collected_ids[3].id,
                "movie": f.file_path,
                "mrc_out": f.mrc_out,
                "pixel_size": proc_params.angpix,
                "image_number": f.image_number,
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[2 * i],
                "ft_bin": proc_params.motion_corr_binning,
                "fm_dose": proc_params.dose_per_frame,
                "gain_ref": proc_params.gain_ref,
                "picker_uuid": murfey_ids[2 * i + 1],
                "session_id": session_id,
                "particle_diameter": proc_params.particle_diameter or 0,
                "fm_int_file": (
                    proc_params.eer_fractionation_file
                    if proc_params.eer_fractionation_file
                    else f.eer_fractionation_file
                ),
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "cryolo_model_weights": str(
                    cryolo_model_path(
                        murfey_session.visit, murfey_session.instrument_name
                    )
                ),
                "foil_hole_id": foil_hole_id,
            },
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"]["feedback_queue"] = (
                murfey.server._transport_object.feedback_queue
            )
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
            murfey_db.delete(f)
        else:
            logger.error(
                f"Pre-processing was requested for {ppath.name} but no Zocalo transport object was found"
            )
    murfey_db.commit()
    murfey_db.close()
    return {"success": True}
