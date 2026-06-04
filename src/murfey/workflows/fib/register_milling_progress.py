from __future__ import annotations

import json
import logging
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
from murfey.util.models import (
    GridSquareParameters,
    LamellaSiteInfo,
    MillingStepInfo,
    MillingSteps,
    StagePositionInfo,
    StagePositionValues,
)

if TYPE_CHECKING:
    from murfey.server.ispyb import TransportManager


logger = logging.getLogger("murfey.workflows.fib.register_milling_progress")


def _ensure_prerequisites(
    session_id: int,
    instrument_name: str,
    visit_name: str,
    project_name: str,
    slot_number: int,
    site_number: int,
    transport_object: TransportManager,
    murfey_db: SQLModelSession,
):
    """
    Uses the FIB milling metadata provided to create the necessary DataCollectionGroup,
    Atlas, and GridSquare placeholders in Murfey if they don't already exist

    """
    # Construct the DataCollectionGroup and GridSquare lookup tags
    dcg_tag = f"{project_name}--slot_{slot_number}"

    # Determine variables to register data collection group and atlas with
    proposal_code = "".join(char for char in visit_name.split("-")[0] if char.isalpha())
    proposal_number = "".join(
        char for char in visit_name.split("-")[0] if char.isdigit()
    )
    visit_number = visit_name.split("-")[-1]

    # Register the DataCollectionGroup and Atlas placeholder if it doesn't already exist
    if (
        murfey_db.exec(
            select(MurfeyDB.DataCollectionGroup)
            .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
            .where(MurfeyDB.DataCollectionGroup.tag == dcg_tag)
        ).one_or_none()
        is None
    ):
        dcg_message = {
            "microscope": instrument_name,
            "proposal_code": proposal_code,
            "proposal_number": proposal_number,
            "visit_number": visit_number,
            "session_id": session_id,
            "tag": dcg_tag,
            "experiment_type_id": 46,
            "atlas": "",
            "atlas_pixel_size": 0.0,
            "sample": slot_number,
            "color_flags": None,
            "collection_mode": None,
        }
        if entry_point_result := entry_points(
            group="murfey.workflows", name="data_collection_group"
        ):
            (workflow,) = entry_point_result
            _ = workflow.load()(
                message=dcg_message,
                murfey_db=murfey_db,
            )

    # Register the GridSquare if it doesn't already exist
    grid_square_entry = murfey_db.exec(
        select(MurfeyDB.GridSquare)
        .where(MurfeyDB.GridSquare.session_id == session_id)
        .where(MurfeyDB.GridSquare.name == site_number)
        .where(MurfeyDB.GridSquare.tag == dcg_tag)
    ).one_or_none()
    if grid_square_entry is None:
        dcg_entry = murfey_db.exec(
            select(MurfeyDB.DataCollectionGroup)
            .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
            .where(MurfeyDB.DataCollectionGroup.tag == dcg_tag)
        ).one()
        grid_square_ispyb_result = transport_object.do_insert_grid_square(
            atlas_id=dcg_entry.atlas_id,
            grid_square_id=site_number,
            grid_square_parameters=GridSquareParameters(
                tag=dcg_tag,
            ),
        )
        # Register to Murfey
        grid_square_entry = MurfeyDB.GridSquare(
            id=grid_square_ispyb_result.get("return_value", None),
            name=site_number,
            session_id=session_id,
            tag=dcg_tag,
        )
        murfey_db.add(grid_square_entry)
        murfey_db.commit()

    return grid_square_entry


MILLING_STEP_LOOKUP = (
    # Match the MillingStep key names to their ISPyB IDs
    # Preparation stage
    (
        (
            "eucentric_tilt",
            "artificial_features",
            "milling_angle",
            "image_acquisition",
            "lamella_placement",
        ),
        "preparation_site",
    ),
    # Milling stage
    (
        (
            "delay_1",
            "reference_definition",
            "reference_definition_electron",
            "stress_relief_cuts",
            "reference_redefinition_1",
            "rough_milling",
            "rough_milling_electron",
            "reference_redefinition_2",
            "medium_milling",
            "medium_milling_electron",
            "fine_milling",
            "fine_milling_electron",
            "finer_milling",
            "finer_milling_electron",
        ),
        "chunk_site",
    ),
    # Thinning stage
    (
        (
            "delay_2",
            "polishing_1",
            "polishing_1_electron",
            "polishing_2",
            "polishing_2_ion",
            "polishing_2_electron",
        ),
        "thinning_site",
    ),
)


def _register_milling_step(
    milling_steps: MillingSteps,
    stage_info: StagePositionInfo,
    grid_square: MurfeyDB.GridSquare,
    transport_object: TransportManager,
    murfey_db: SQLModelSession,
):
    """
    Registers FIB milling metadata for the current lamella site as MillingStep entries
    in ISPyB. If successful, will proceed to create a backup copy of the inserted row
    in Murfey.
    """
    # Check that GridSquare has ID (for type checking)
    if grid_square.id is None:
        logger.error("Current GridSquare entry has no ID")
        return None

    # Iteratively go through the LamellaSiteInfo model and insert for each step
    for steps, stage_name in MILLING_STEP_LOOKUP:
        for step_name in steps:
            step_info: MillingStepInfo | None = milling_steps.__getattribute__(
                step_name
            )
            # Early continues if key information is missing
            if step_info is None:
                logger.debug(f"No step info found for {step_name}")
                continue
            if step_info.recipe_name is None:
                logger.debug(f"No recipe name found for {step_name}")
                continue
            if step_info.step_name is None:
                logger.debug(f"No step name found for {step_name}")
                continue

            stage_values: StagePositionValues | None = stage_info.__getattribute__(
                stage_name
            )
            if stage_values is None:
                stage_values = StagePositionValues()

            # Check if the step has already been registered in Murfey
            milling_step_entry = murfey_db.exec(
                select(MurfeyDB.MillingStep)
                .where(MurfeyDB.MillingStep.grid_square_id == grid_square.id)
                .where(MurfeyDB.MillingStep.recipe_name == step_info.recipe_name)
                .where(MurfeyDB.MillingStep.activity_name == step_info.step_name)
            ).one_or_none()

            if milling_step_entry is None:
                # Create a new ISPyB entry if no Murfey one is found
                result = transport_object.do_insert_milling_step(
                    # IDs
                    recipe_name=step_info.recipe_name,
                    activity_name=step_info.step_name,
                    grid_square_id=grid_square.id,
                    # Values
                    is_enabled=step_info.is_enabled,
                    status=step_info.status,
                    execution_time=step_info.execution_time,
                    stage_x=stage_values.x,
                    stage_y=stage_values.y,
                    stage_z=stage_values.z,
                    rotation=stage_values.rotation,
                    tilt_alpha=stage_values.tilt_alpha,
                    beam_type=step_info.beam_type,
                    beam_voltage=step_info.voltage,
                    beam_current=step_info.current,
                    milling_angle=step_info.milling_angle,
                    depth_correction=step_info.depth_correction,
                    lamella_offset=step_info.lamella_offset,
                    trench_height_front=step_info.trench_height_front,
                    trench_height_rear=step_info.trench_height_rear,
                    width_overlap_front_left=step_info.width_overlap_front_left,
                    width_overlap_front_right=step_info.width_overlap_front_right,
                    width_overlap_rear_left=step_info.width_overlap_rear_left,
                    width_overlap_rear_right=step_info.width_overlap_rear_right,
                )
                if result.get("return_value") is None:
                    logger.error(
                        f"No MillingStep entry created for {step_info.step_name}"
                    )
                    continue

                # Create a corresponding record in Murfey
                milling_step_entry = MurfeyDB.MillingStep(
                    id=int(result["return_value"]),
                    grid_square_id=grid_square.id,
                    recipe_name=step_info.recipe_name,
                    activity_name=step_info.step_name,
                    is_enabled=step_info.is_enabled,
                    status=step_info.status,
                    execution_time=step_info.execution_time,
                    stage_x=stage_values.x,
                    stage_y=stage_values.y,
                    stage_z=stage_values.z,
                    rotation=stage_values.rotation,
                    tilt_alpha=stage_values.tilt_alpha,
                    beam_type=step_info.beam_type,
                    beam_voltage=step_info.voltage,
                    beam_current=step_info.current,
                    milling_angle=step_info.milling_angle,
                    depth_correction=step_info.depth_correction,
                    lamella_offset=step_info.lamella_offset,
                    trench_height_front=step_info.trench_height_front,
                    trench_height_rear=step_info.trench_height_rear,
                    width_overlap_front_left=step_info.width_overlap_front_left,
                    width_overlap_front_right=step_info.width_overlap_front_right,
                    width_overlap_rear_left=step_info.width_overlap_rear_left,
                    width_overlap_rear_right=step_info.width_overlap_rear_right,
                )
            else:
                # Update the existing ISPyB one if it already exists
                result = transport_object.do_update_milling_step(
                    milling_step_id=milling_step_entry.id,
                    is_enabled=step_info.is_enabled,
                    status=step_info.status,
                    execution_time=step_info.execution_time,
                    stage_x=stage_values.x,
                    stage_y=stage_values.y,
                    stage_z=stage_values.z,
                    rotation=stage_values.rotation,
                    tilt_alpha=stage_values.tilt_alpha,
                    beam_type=step_info.beam_type,
                    beam_voltage=step_info.voltage,
                    beam_current=step_info.current,
                    milling_angle=step_info.milling_angle,
                    depth_correction=step_info.depth_correction,
                    lamella_offset=step_info.lamella_offset,
                    trench_height_front=step_info.trench_height_front,
                    trench_height_rear=step_info.trench_height_rear,
                    width_overlap_front_left=step_info.width_overlap_front_left,
                    width_overlap_front_right=step_info.width_overlap_front_right,
                    width_overlap_rear_left=step_info.width_overlap_rear_left,
                    width_overlap_rear_right=step_info.width_overlap_rear_right,
                )
                if result.get("return_value", None) is None:
                    logger.error(
                        f"Could not update MillingStep entry for {step_info.step_name}"
                    )
                    continue

                # Update the existing Murfey one
                milling_step_entry.is_enabled = step_info.is_enabled
                milling_step_entry.status = step_info.status
                milling_step_entry.execution_time = step_info.execution_time
                milling_step_entry.stage_x = stage_values.x
                milling_step_entry.stage_y = stage_values.y
                milling_step_entry.stage_z = stage_values.z
                milling_step_entry.rotation = stage_values.rotation
                milling_step_entry.tilt_alpha = stage_values.tilt_alpha
                milling_step_entry.beam_type = step_info.beam_type
                milling_step_entry.beam_voltage = step_info.voltage
                milling_step_entry.beam_current = step_info.current
                milling_step_entry.milling_angle = step_info.milling_angle
                milling_step_entry.depth_correction = step_info.depth_correction
                milling_step_entry.lamella_offset = step_info.lamella_offset
                milling_step_entry.trench_height_front = step_info.trench_height_front
                milling_step_entry.trench_height_rear = step_info.trench_height_rear
                milling_step_entry.width_overlap_front_left = (
                    step_info.width_overlap_front_left
                )
                milling_step_entry.width_overlap_front_right = (
                    step_info.width_overlap_front_right
                )
                milling_step_entry.width_overlap_rear_left = (
                    step_info.width_overlap_rear_left
                )
                milling_step_entry.width_overlap_rear_right = (
                    step_info.width_overlap_rear_right
                )
            # Mark entry for committing
            murfey_db.add(milling_step_entry)

    # Commit all changes at once
    murfey_db.commit()
    return None


def run(message: dict[str, Any], murfey_db: SQLModelSession):
    # Early exit if no TransportManager was set up
    if _transport_object is None:
        logger.error("No TransportManager object was configured")
        return {"success": False, "requeue": False}

    try:
        # Parse and unpack incoming message
        session_id = int(message["session_id"])
        site_info = LamellaSiteInfo(**message["site_info"])
        logger.debug(
            "Received the following FIB metadata for registration:\n"
            f"{json.dumps(site_info.model_dump(exclude_none=True), indent=2, default=str)}"
        )
    except Exception:
        logger.error("Error parsing contents of message", exc_info=True)
        return {"success": False, "requeue": False}

    # Early exits if information needed to construct lookup tags are missing
    # Project and site values
    if site_info.project_name is None:
        logger.error("Could not construct lookup tags; 'project_name' is missing")
        return {"success": False, "requeue": False}
    project_name = site_info.project_name
    if site_info.site_number is None:
        logger.error("Could not construct lookup tags; 'site_number' is missing")
        return {"success": False, "requeue": False}
    site_number = site_info.site_number
    if site_info.site_name is None:
        logger.error("Could not construct lookup tags; 'site_name' is missing")
        return {"success": False, "requeue": False}
    site_name = site_info.site_name

    # Stage information
    if site_info.stage_info is None:
        logger.error("Could not construct lookup tags; 'stage_info' is missing")
        return {"success": False, "requeue": False}
    stage_info = site_info.stage_info
    if stage_info.preparation_site is None:
        logger.error("Could not construct lookup tags; 'preparation_site' is missing")
        return {"success": False, "requeue": False}
    preparation_site = stage_info.preparation_site
    if preparation_site.slot_number is None:
        logger.error("Could not construct lookup tags; 'slot_number' is missing")
        return {"success": False, "requeue": False}
    slot_number = preparation_site.slot_number

    # Milling step information
    if site_info.steps is None:
        logger.error("No milling step info found in current message")
        return None
    milling_steps = site_info.steps

    # Outer try-finally block to handle database cleanup
    try:
        try:
            # Load instrument name and visit ID
            murfey_session = murfey_db.exec(
                select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
            ).one()
            visit_name = murfey_session.visit
            instrument_name = murfey_session.instrument_name
        except Exception:
            logger.error(
                "Exception encountered while querying Murfey database", exc_info=True
            )
            return {"success": False, "requeue": False}

        try:
            # Register the prerequisite information for this site
            grid_square_entry = _ensure_prerequisites(
                session_id=session_id,
                instrument_name=instrument_name,
                visit_name=visit_name,
                project_name=project_name,
                slot_number=slot_number,
                site_number=site_number,
                transport_object=_transport_object,
                murfey_db=murfey_db,
            )
        except Exception:
            logger.error(
                "Exception encountered while registering preqrequisite database entries",
                exc_info=True,
            )
            return {"success": False, "requeue": False}
        if grid_square_entry is None:
            logger.error(
                f"Could not create GridSquare database entry for site {site_name}"
            )
            return {"success": False, "requeue": False}

        try:
            # Insert or update MillingStep entries
            _register_milling_step(
                milling_steps=milling_steps,
                stage_info=stage_info,
                grid_square=grid_square_entry,
                transport_object=_transport_object,
                murfey_db=murfey_db,
            )
        except Exception:
            logger.error(
                "Exception encountered while registering milling progress",
                exc_info=True,
            )
            return {"success": False, "requeue": False}
        logger.info(f"Successfully registered milling progress of site {site_name}")
        return {"success": True}
    finally:
        murfey_db.close()
