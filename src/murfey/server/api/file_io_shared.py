from datetime import datetime
from logging import getLogger
from pathlib import Path

from pydantic import BaseModel
from sqlmodel import select
from werkzeug.utils import secure_filename

from murfey.server.gain import Camera, prepare_eer_gain, prepare_gain
from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import Session

logger = getLogger("murfey.server.api.file_io_shared")


class GainReference(BaseModel):
    gain_ref: Path
    rescale: bool = True
    eer: bool = False
    tag: str = ""


async def process_gain(
    session_id: int, gain_reference_params: GainReference, db=murfey_db
):
    murfey_session = db.exec(select(Session).where(Session.id == session_id)).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    camera = getattr(Camera, machine_config.camera)
    if gain_reference_params.eer:
        executables = machine_config.external_executables_eer
    else:
        executables = machine_config.external_executables
    env = machine_config.external_environment
    safe_path_name = secure_filename(gain_reference_params.gain_ref.name)
    filepath = (
        Path(machine_config.rsync_basepath)
        / str(datetime.now().year)
        / secure_filename(visit_name)
        / machine_config.gain_directory_name
    )

    # Check under previous year if the folder doesn't exist
    if not filepath.exists():
        filepath_prev = filepath
        filepath = (
            Path(machine_config.rsync_basepath)
            / str(datetime.now().year - 1)
            / secure_filename(visit_name)
            / machine_config.gain_directory_name
        )
        # If it's not in the previous year, it's a genuine error
        if not filepath.exists():
            log_message = (
                "Unable to find gain reference directory under "
                f"{str(filepath_prev)!r} or {str(filepath)}"
            )
            logger.error(log_message)
            raise FileNotFoundError(log_message)

    if gain_reference_params.eer:
        new_gain_ref, new_gain_ref_superres = await prepare_eer_gain(
            filepath / safe_path_name,
            executables,
            env,
            tag=gain_reference_params.tag,
        )
    else:
        new_gain_ref, new_gain_ref_superres = await prepare_gain(
            camera,
            filepath / safe_path_name,
            executables,
            env,
            rescale=gain_reference_params.rescale,
            tag=gain_reference_params.tag,
        )
    if new_gain_ref and new_gain_ref_superres:
        return {
            "gain_ref": new_gain_ref.relative_to(Path(machine_config.rsync_basepath)),
            "gain_ref_superres": new_gain_ref_superres.relative_to(
                Path(machine_config.rsync_basepath)
            ),
        }
    elif new_gain_ref:
        return {
            "gain_ref": new_gain_ref.relative_to(Path(machine_config.rsync_basepath)),
            "gain_ref_superres": None,
        }
    else:
        return {"gain_ref": str(filepath / safe_path_name), "gain_ref_superres": None}
