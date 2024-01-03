from typing import Dict

from fastapi import APIRouter

from murfey.client.multigrid_control import MultigridController
from murfey.client.rsync import RSyncer
from murfey.client.watchdir_multigrid import MultigridDirWatcher
from murfey.util.instrument_models import MultigridWatcherSpec

router = APIRouter()

watchers = {}
rsyncers: Dict[str, RSyncer] = {}
controllers = {}


def get_machine_config():
    return {}


@router.get("/health")
def health():
    return True


@router.post("/sessions/{session_id}/multigrid_watcher/{label}")
def start_multigrid_watcher(
    session_id: int, label: str, watcher_spec: MultigridWatcherSpec
) -> bool:
    controllers[label] = MultigridController([], demo=True)
    watchers[label] = MultigridDirWatcher(
        watcher_spec.source,
        watcher_spec.configuration,
        skip_existing_processing=watcher_spec.skip_existing_processing,
    )
    watchers[label].subscribe(controllers[label]._start_rsyncer_multigrid)
    watchers[label].start()
    return True


@router.delete("/sessions/{session_id}/multigrid_watcher/{label}")
def stop_multigrid_watcher(session_id: int, label: str):
    watchers[label].request_stop()
