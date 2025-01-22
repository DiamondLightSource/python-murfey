import logging
from pathlib import Path
from typing import Optional

from PIL import Image
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select
from werkzeug.utils import secure_filename

from murfey.server import _murfey_id, _transport_object, sanitise
from murfey.server.api.auth import MurfeySessionID
from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config, get_microscope
from murfey.util.db import DataCollectionGroup, FoilHole, GridSquare
from murfey.util.models import FoilHoleParameters, GridSquareParameters
from murfey.util.processing_params import default_spa_parameters
from murfey.util.spa_metadata import (
    GridSquareInfo,
    foil_hole_data,
    foil_hole_from_file,
    get_grid_square_atlas_positions,
    grid_square_data,
    grid_square_from_file,
)

logger = logging.getLogger("murfey.workflows.spa.flush_spa_preprocess")


def register_grid_square(
    session_id: MurfeySessionID,
    gsid: int,
    grid_square_params: GridSquareParameters,
    db=murfey_db,
):
    try:
        grid_square = db.exec(
            select(GridSquare)
            .where(GridSquare.name == gsid)
            .where(GridSquare.tag == grid_square_params.tag)
            .where(GridSquare.session_id == session_id)
        ).one()
        grid_square.x_location = grid_square_params.x_location
        grid_square.y_location = grid_square_params.y_location
        grid_square.x_stage_position = grid_square_params.x_stage_position
        grid_square.y_stage_position = grid_square_params.y_stage_position
        if _transport_object:
            _transport_object.do_update_grid_square(grid_square.id, grid_square_params)
    except Exception:
        if _transport_object:
            dcg = db.exec(
                select(DataCollectionGroup)
                .where(DataCollectionGroup.session_id == session_id)
                .where(DataCollectionGroup.tag == grid_square_params.tag)
            ).one()
            gs_ispyb_response = _transport_object.do_insert_grid_square(
                dcg.atlas_id, gsid, grid_square_params
            )
        else:
            # mock up response so that below still works
            gs_ispyb_response = {"success": False, "return_value": None}
        secured_grid_square_image_path = secure_filename(grid_square_params.image)
        if (
            secured_grid_square_image_path
            and Path(secured_grid_square_image_path).is_file()
        ):
            jpeg_size = Image.open(secured_grid_square_image_path).size
        else:
            jpeg_size = (0, 0)
        grid_square = GridSquare(
            id=(
                gs_ispyb_response["return_value"]
                if gs_ispyb_response["success"]
                else None
            ),
            name=gsid,
            session_id=session_id,
            tag=grid_square_params.tag,
            x_location=grid_square_params.x_location,
            y_location=grid_square_params.y_location,
            x_stage_position=grid_square_params.x_stage_position,
            y_stage_position=grid_square_params.y_stage_position,
            readout_area_x=grid_square_params.readout_area_x,
            readout_area_y=grid_square_params.readout_area_y,
            thumbnail_size_x=grid_square_params.thumbnail_size_x or jpeg_size[0],
            thumbnail_size_y=grid_square_params.thumbnail_size_y or jpeg_size[1],
            pixel_size=grid_square_params.pixel_size,
            image=secured_grid_square_image_path,
        )
    db.add(grid_square)
    db.commit()
    db.close()


def register_foil_hole(
    session_id: MurfeySessionID,
    gs_name: int,
    foil_hole_params: FoilHoleParameters,
    db=murfey_db,
):
    try:
        gs = db.exec(
            select(GridSquare)
            .where(GridSquare.tag == foil_hole_params.tag)
            .where(GridSquare.session_id == session_id)
            .where(GridSquare.name == gs_name)
        ).one()
        gsid = gs.id
    except NoResultFound:
        logger.debug(
            f"Foil hole {sanitise(str(foil_hole_params.name))} could not be registered as grid square {sanitise(str(gs_name))} was not found"
        )
        return
    secured_foil_hole_image_path = secure_filename(foil_hole_params.image)
    if foil_hole_params.image and Path(secured_foil_hole_image_path).is_file():
        jpeg_size = Image.open(secured_foil_hole_image_path).size
    else:
        jpeg_size = (0, 0)
    try:
        foil_hole = db.exec(
            select(FoilHole)
            .where(FoilHole.name == foil_hole_params.name)
            .where(FoilHole.grid_square_id == gsid)
            .where(FoilHole.session_id == session_id)
        ).one()
        foil_hole.x_location = foil_hole_params.x_location
        foil_hole.y_location = foil_hole_params.y_location
        foil_hole.x_stage_position = foil_hole_params.x_stage_position
        foil_hole.y_stage_position = foil_hole_params.y_stage_position
        foil_hole.readout_area_x = foil_hole_params.readout_area_x
        foil_hole.readout_area_y = foil_hole_params.readout_area_y
        foil_hole.thumbnail_size_x = foil_hole_params.thumbnail_size_x or jpeg_size[0]
        foil_hole.thumbnail_size_y = foil_hole_params.thumbnail_size_y or jpeg_size[1]
        foil_hole.pixel_size = foil_hole_params.pixel_size
        if _transport_object:
            _transport_object.do_update_foil_hole(
                foil_hole.id, gs.thumbnail_size_x / gs.readout_area_x, foil_hole_params
            )
    except Exception:
        if _transport_object:
            fh_ispyb_response = _transport_object.do_insert_foil_hole(
                gs.id, gs.thumbnail_size_x / gs.readout_area_x, foil_hole_params
            )
        else:
            fh_ispyb_response = {"success": False, "return_value": None}
        foil_hole = FoilHole(
            id=(
                fh_ispyb_response["return_value"]
                if fh_ispyb_response["success"]
                else None
            ),
            name=foil_hole_params.name,
            session_id=session_id,
            grid_square_id=gsid,
            x_location=foil_hole_params.x_location,
            y_location=foil_hole_params.y_location,
            x_stage_position=foil_hole_params.x_stage_position,
            y_stage_position=foil_hole_params.y_stage_position,
            readout_area_x=foil_hole_params.readout_area_x,
            readout_area_y=foil_hole_params.readout_area_y,
            thumbnail_size_x=foil_hole_params.thumbnail_size_x or jpeg_size[0],
            thumbnail_size_y=foil_hole_params.thumbnail_size_y or jpeg_size[1],
            pixel_size=foil_hole_params.pixel_size,
            image=secured_foil_hole_image_path,
        )
    db.add(foil_hole)
    db.commit()
    db.close()


def _grid_square_metadata_file(f: Path, grid_square: int) -> Optional[Path]:
    """Search through metadata directories to find the required grid square dm"""
    raw_dir = f.parent.parent.parent
    metadata_dirs = raw_dir.glob("metadata*")
    for md_dir in metadata_dirs:
        gs_path = md_dir / f"Metadata/GridSquare_{grid_square}.dm"
        if gs_path.is_file():
            return gs_path
    logger.error(f"Could not determine grid square metadata path for {f}")
    return None


def _flush_position_analysis(
    movie_path: Path, dcg_id: int, session_id: int, db: Session
) -> Optional[int]:
    """Register a grid square and foil hole in the database"""
    data_collection_group = murfey_db.exec(
        select(db.DataCollectionGroup).where(db.DataCollectionGroup.id == dcg_id)
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
    register_foil_hole(session_id, gs.id, foil_hole_parameters, murfey_db)
    return foil_hole


def flush_spa_preprocessing(message: dict, db: Session, demo: bool = False):
    session_id = message["session_id"]
    stashed_files = murfey_db.exec(
        select(db.PreprocessStash)
        .where(db.PreprocessStash.session_id == session_id)
        .where(db.PreprocessStash.tag == message["tag"])
    ).all()
    if not stashed_files:
        return None
    instrument_name = (
        murfey_db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    recipe_name = machine_config.recipes.get("em-spa-preprocess", "em-spa-preprocess")
    collected_ids = murfey_db.exec(
        select(
            db.DataCollectionGroup,
            db.DataCollection,
            db.ProcessingJob,
            db.AutoProcProgram,
        )
        .where(db.DataCollectionGroup.session_id == session_id)
        .where(db.DataCollectionGroup.tag == message["tag"])
        .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
        .where(db.ProcessingJob.dc_id == db.DataCollection.id)
        .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
        .where(db.ProcessingJob.recipe == recipe_name)
    ).one()
    params = murfey_db.exec(
        select(db.SPARelionParameters, db.SPAFeedbackParameters)
        .where(db.SPARelionParameters.pj_id == collected_ids[2].id)
        .where(db.SPAFeedbackParameters.pj_id == db.SPARelionParameters.pj_id)
    ).one()
    proc_params = params[0]
    feedback_params = params[1]
    if not proc_params:
        logger.warning(
            f"No SPA processing parameters found for client processing job ID {collected_ids[2].id}"
        )
        raise ValueError(
            "No processing parameters were found in the database when flushing SPA preprocessing"
        )

    murfey_ids = _murfey_id(
        collected_ids[3].id,
        murfey_db,
        number=2 * len(stashed_files),
        close=False,
    )
    if feedback_params.picker_murfey_id is None:
        feedback_params.picker_murfey_id = murfey_ids[1]
        murfey_db.add(feedback_params)

    for i, f in enumerate(stashed_files):
        if f.foil_hole_id:
            foil_hole_id = f.foil_hole_id
        else:
            # Register grid square and foil hole if not present
            try:
                foil_hole_id = _flush_position_analysis(
                    movie_path=f.file_path,
                    dcg_id=collected_ids[0].id,
                    session_id=session_id,
                    db=db,
                )
            except Exception as e:
                logger.error(
                    f"Flushing position analysis for {f.file_path} caused exception {e}", exc_info=True
                )
                foil_hole_id = None

        mrcp = Path(f.mrc_out)
        ppath = Path(f.file_path)
        if not mrcp.parent.exists():
            mrcp.parent.mkdir(parents=True)
        movie = db.Movie(
            murfey_id=murfey_ids[2 * i],
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
                "fm_int_file": f.eer_fractionation_file,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "foil_hole_id": foil_hole_id,
            },
        }
        if _transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = _transport_object.feedback_queue
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
            murfey_db.delete(f)
        else:
            logger.error(
                f"Pre-processing was requested for {ppath.name} but no Zocalo transport object was found"
            )
    murfey_db.commit()
    murfey_db.close()
    return None
