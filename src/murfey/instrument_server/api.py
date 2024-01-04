from logging import getLogger
from pathlib import Path
from typing import Dict

from fastapi import APIRouter
from pydantic import BaseModel

from murfey.client.multigrid_control import MultigridController
from murfey.client.rsync import RSyncer
from murfey.client.watchdir_multigrid import MultigridDirWatcher
from murfey.util.instrument_models import MultigridWatcherSpec

logger = getLogger("murfey.instrument_server.api")

router = APIRouter()

watchers = {}
rsyncers: Dict[str, RSyncer] = {}
controllers = {}


def get_machine_config():
    return {}


@router.get("/health")
def health():
    return True


@router.post("/sessions/{session_id}/multigrid_watcher")
def start_multigrid_watcher(
    session_id: int, watcher_spec: MultigridWatcherSpec
) -> bool:
    label = watcher_spec.label
    controllers[label] = MultigridController(
        [], watcher_spec.visit, session_id, demo=True, do_transfer=False
    )
    watchers[label] = MultigridDirWatcher(
        watcher_spec.source,
        watcher_spec.configuration.dict(),
        skip_existing_processing=watcher_spec.skip_existing_processing,
    )
    watchers[label].subscribe(controllers[label]._start_rsyncer_multigrid)
    watchers[label].start()
    return True


@router.delete("/sessions/{session_id}/multigrid_watcher/{label}")
def stop_multigrid_watcher(session_id: int, label: str):
    watchers[label].request_stop()


class RsyncerSource(BaseModel):
    source: Path
    label: str


@router.post("/sessions/{session_id}/stop_rsyncer")
def stop_rsyncer(session_id: int, rsyncer_source: RsyncerSource):
    controllers[rsyncer_source.label].rsync_processes[
        rsyncer_source.source
    ]._halt_thread = True
