import json
import logging
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session as SQLModelSession, select

import murfey.server
import murfey.util.db as MurfeyDB
from murfey.server.api.auth import validate_instrument_token
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise_path
from murfey.util.config import get_machine_config

logger = logging.getLogger("murfey.server.api.workflow_sim")

router = APIRouter(
    prefix="/workflow/sim",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: CryoSIM"],
)


class SIMDataFile(BaseModel):
    file: Path


@router.post("/sessions/{session_id}/sim_recon")
def request_sim_reconstruction(
    session_id: int,
    sim_data: SIMDataFile,
    murfey_db: SQLModelSession = murfey_db,
):
    if murfey.server._transport_object is None:
        logger.error("No TransportManager object was set up")
        return None

    # Load instrument and visit information based on session
    try:
        murfey_session = murfey_db.exec(
            select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
        ).one()
        instrument_name = murfey_session.instrument_name
        visit_name = murfey_session.visit
        otf_dir = Path(murfey_session.current_gain_ref)
    except Exception:
        logger.error("Error querying session information from database", exc_info=True)
        return None

    # Look for OTF files in the saved directory and match them to wavelengths
    COLOR_LOOKUP = {
        452: "blue",
        525: "green",
        605: "red",
        655: "far_red",
    }
    otf_files: dict[str, Path] = {}
    pattern = r"(?<!\d)\d{3}(?!\d)"  # Regex match for EXACTLY 3 consecutive digits (wavelength)
    for file in otf_dir.glob("*"):
        if (
            file.is_file()
            and file.suffix.endswith((".tif", ".tiff"))
            and "otf" in file.stem.lower()
            and (match := re.search(pattern, file.stem))
        ):
            wavelength = int(match.group())
            if color := COLOR_LOOKUP.get(wavelength):
                otf_files[color] = file

    # If 4 matches weren't found, log as an error and exit early
    if len(otf_files) < 4:
        logger.error(
            f"One or more OTF files missing from {otf_dir}\n"
            f"Files accounted for:\n"
            f"{json.dumps(otf_files, indent=2, default=str)}"
        )
        return None

    # Load PySIMRecon values from the machine config
    try:
        machine_config = get_machine_config(instrument_name)[instrument_name]
        pysimrecon_config: dict[str, dict[str, Any]] | None = (
            machine_config.calibrations.get("pysimrecon_config")
        )
        if not pysimrecon_config:
            # If no calibration was provided, use defaults
            # Values provided on 2026-07-16
            pysimrecon_config = {
                "blue": {
                    "wavelength": 452,
                    "ls": 0.330,
                    "beaddiam": 0.220,
                },
                "green": {
                    "wavelength": 525,
                    "ls": 0.394,
                },
                "red": {
                    "wavelength": 605,
                    "ls": 0.451,
                },
                "far_red": {
                    "wavelength": 655,
                    "ls": 0.521,
                },
            }
            logger.warning(
                f"No PySIMRecon configuration found for {instrument_name}; "
                f"using known defaults \n{json.dumps(pysimrecon_config, indent=2)}"
            )
    except Exception:
        logger.error("Error loading machine config from database", exc_info=True)
        return None

    # Add the OTF file information to the PySIMRecon config
    for color, otf_file in otf_files.items():
        pysimrecon_config[color]["otf_path"] = str(otf_file)

    # Construct message and submit it to 'processing_recipe'
    logger.info(
        f"Submitting request to process the cryoSIM file {sanitise_path(sim_data.file)}"
    )
    # Construct the output directory for the PySIMRecon outputs to be saved to
    try:
        visit_idx = sim_data.file.parts.index(visit_name)
        raw_dir = Path(
            "/".join(
                ""
                if part == "/"  # Replace root "/" with "" for Linux paths
                else part
                for part in sim_data.file.parts[: visit_idx + 2]
            )
        )
        output_dir = (
            raw_dir.parent / "processed" / sim_data.file.parent.relative_to(raw_dir)
        )
    except Exception:
        logger.error(
            "Could not determine the output directory to save the cryoSIM file "
            f"{sanitise_path(sim_data.file)} to"
        )
        return None
    recipe = {
        "recipes": ["sim-reconstruction"],
        "parameters": {
            # PySIMRecon parameters
            "visit_name": visit_name,
            "file": f"{str(sim_data.file)}",
            "output_dir": str(output_dir),
            "blue_params": str(pysimrecon_config["blue"]),
            "green_params": str(pysimrecon_config["green"]),
            "red_params": str(pysimrecon_config["red"]),
            "far_red_params": str(pysimrecon_config["far_red"]),
            # Return message
            "session_id": session_id,
            "feedback_queue": murfey.server._transport_object.feedback_queue,
        },
    }
    logger.debug(
        "Will submit the following message to 'processing_recipe':\n"
        f"{json.dumps(recipe, indent=2, default=str)}"
    )
    # Submit message for processing
    murfey.server._transport_object.send(
        queue="processing_recipe", message=recipe, new_connection=True
    )
