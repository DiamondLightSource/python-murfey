import logging
from pathlib import Path
from typing import Optional

from PIL import Image
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionIDInstrument as MurfeySessionID
from murfey.server.feedback import _murfey_id
from murfey.util import sanitise, secure_path
from murfey.util.config import get_machine_config, get_microscope
from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    Movie,
    PreprocessStash,
    ProcessingJob,
)
from murfey.util.db import Session as MurfeySession
from murfey.util.db import SPAFeedbackParameters, SPARelionParameters
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

logger = logging.getLogger("murfey.workflows.spa.flush_spa_preprocess")


def register_grid_square(
    session_id: MurfeySessionID,
    gsid: int,
    grid_square_params: GridSquareParameters,
    murfey_db: Session,
):
    try:
        grid_square = murfey_db.exec(
            select(GridSquare)
            .where(GridSquare.name == gsid)
            .where(GridSquare.tag == grid_square_params.tag)
            .where(GridSquare.session_id == session_id)
        ).one()
        grid_square.x_location = grid_square_params.x_location or grid_square.x_location
        grid_square.y_location = grid_square_params.y_location or grid_square.y_location
        grid_square.x_stage_position = (
            grid_square_params.x_stage_position or grid_square.x_stage_position
        )
        grid_square.y_stage_position = (
            grid_square_params.y_stage_position or grid_square.y_stage_position
        )
        grid_square.readout_area_x = (
            grid_square_params.readout_area_x or grid_square.readout_area_x
        )
        grid_square.readout_area_y = (
            grid_square_params.readout_area_y or grid_square.readout_area_y
        )
        grid_square.thumbnail_size_x = (
            grid_square_params.thumbnail_size_x or grid_square.thumbnail_size_x
        )
        grid_square.thumbnail_size_y = (
            grid_square_params.thumbnail_size_y or grid_square.thumbnail_size_y
        )
        grid_square.pixel_size = grid_square_params.pixel_size or grid_square.pixel_size
        grid_square.image = grid_square_params.image or grid_square.image
        if _transport_object:
            _transport_object.do_update_grid_square(grid_square.id, grid_square_params)
    except Exception:
        if _transport_object:
            dcg = murfey_db.exec(
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
        secured_grid_square_image_path = secure_path(Path(grid_square_params.image))
        if secured_grid_square_image_path and secured_grid_square_image_path.is_file():
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
            image=str(secured_grid_square_image_path),
        )
    murfey_db.add(grid_square)
    murfey_db.commit()
    murfey_db.close()


def register_foil_hole(
    session_id: MurfeySessionID,
    gs_name: int,
    foil_hole_params: FoilHoleParameters,
    murfey_db: Session,
) -> Optional[int]:
    try:
        gs = murfey_db.exec(
            select(GridSquare)
            .where(GridSquare.tag == foil_hole_params.tag)
            .where(GridSquare.session_id == session_id)
            .where(GridSquare.name == gs_name)
        ).one()
        gsid = gs.id
    except NoResultFound:
        logger.warning(
            f"Foil hole {sanitise(str(foil_hole_params.name))} could not be registered as grid square {sanitise(str(gs_name))} was not found"
        )
        return None
    secured_foil_hole_image_path = secure_path(Path(foil_hole_params.image))
    if foil_hole_params.image and secured_foil_hole_image_path.is_file():
        jpeg_size = Image.open(secured_foil_hole_image_path).size
    else:
        jpeg_size = (0, 0)
    try:
        foil_hole = murfey_db.exec(
            select(FoilHole)
            .where(FoilHole.name == foil_hole_params.name)
            .where(FoilHole.grid_square_id == gsid)
            .where(FoilHole.session_id == session_id)
        ).one()
        foil_hole.x_location = foil_hole_params.x_location or foil_hole.x_location
        foil_hole.y_location = foil_hole_params.y_location or foil_hole.y_location
        foil_hole.x_stage_position = (
            foil_hole_params.x_stage_position or foil_hole.x_stage_position
        )
        foil_hole.y_stage_position = (
            foil_hole_params.y_stage_position or foil_hole.y_stage_position
        )
        foil_hole.readout_area_x = (
            foil_hole_params.readout_area_x or foil_hole.readout_area_x
        )
        foil_hole.readout_area_y = (
            foil_hole_params.readout_area_y or foil_hole.readout_area_y
        )
        foil_hole.thumbnail_size_x = (
            foil_hole_params.thumbnail_size_x or foil_hole.thumbnail_size_x
        ) or jpeg_size[0]
        foil_hole.thumbnail_size_y = (
            foil_hole_params.thumbnail_size_y or foil_hole.thumbnail_size_y
        ) or jpeg_size[1]
        foil_hole.pixel_size = foil_hole_params.pixel_size or foil_hole.pixel_size
        if _transport_object and gs.readout_area_x:
            _transport_object.do_update_foil_hole(
                foil_hole.id, gs.thumbnail_size_x / gs.readout_area_x, foil_hole_params
            )
    except Exception:
        if _transport_object:
            fh_ispyb_response = _transport_object.do_insert_foil_hole(
                gs.id,
                gs.thumbnail_size_x / gs.readout_area_x if gs.readout_area_x else None,
                foil_hole_params,
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
            image=str(secured_foil_hole_image_path),
        )
    fh_id = foil_hole.id
    murfey_db.add(foil_hole)
    murfey_db.commit()
    murfey_db.close()
    return fh_id


def _grid_square_metadata_file(f: Path, grid_square: int) -> Optional[Path]:
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
) -> Optional[int]:
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
    return register_foil_hole(session_id, gs.id, foil_hole_parameters, murfey_db)


def flush_spa_preprocess(message: dict, murfey_db: Session, demo: bool = False) -> bool:
    session_id = message["session_id"]
    stashed_files = murfey_db.exec(
        select(PreprocessStash)
        .where(PreprocessStash.session_id == session_id)
        .where(PreprocessStash.tag == message["tag"])
    ).all()
    if not stashed_files:
        return True

    murfey_session = murfey_db.exec(
        select(MurfeySession).where(MurfeySession.id == message["session_id"])
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
    params = murfey_db.exec(
        select(SPARelionParameters, SPAFeedbackParameters)
        .where(SPARelionParameters.pj_id == collected_ids[2].id)
        .where(SPAFeedbackParameters.pj_id == SPARelionParameters.pj_id)
    ).one()
    proc_params = params[0]
    feedback_params = params[1]
    if not proc_params:
        logger.warning(
            f"No SPA processing parameters found for client processing job ID {collected_ids[2].id}"
        )
        return False

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
                foil_hole_id = _flush_position_analysis(
                    movie_path=Path(f.file_path),
                    dcg_id=collected_ids[0].id,
                    session_id=session_id,
                    murfey_db=murfey_db,
                )
        except Exception as e:
            logger.error(
                f"Flushing position analysis for {f.file_path} caused exception {e}",
                exc_info=True,
            )
            foil_hole_id = None

        mrcp = Path(f.mrc_out)
        ppath = Path(f.file_path)
        if not mrcp.parent.exists():
            mrcp.parent.mkdir(parents=True)
        movie = Movie(
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
    return True
