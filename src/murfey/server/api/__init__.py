from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends
from prometheus_client import Counter, Gauge
from sqlmodel import select

import murfey.server.prometheus as prom
from murfey.server import sanitise
from murfey.server.api.auth import validate_token
from murfey.server.murfey_db import murfey_db
from murfey.util.db import (
    ClientEnvironment,
    MagnificationLookup,
    RsyncInstance,
    Session,
)
from murfey.util.models import ClientInfo, RsyncerInfo, RsyncerSource

log = logging.getLogger("murfey.server.api")

router = APIRouter(dependencies=[Depends(validate_token)])


@router.get("/mag_table/")
def get_mag_table(db=murfey_db) -> List[MagnificationLookup]:
    return db.exec(select(MagnificationLookup)).all()


@router.post("/mag_table/")
def add_to_mag_table(rows: List[MagnificationLookup], db=murfey_db):
    for r in rows:
        db.add(r)
    db.commit()


@router.delete("/mag_table/{mag}")
def remove_mag_table_row(mag: int, db=murfey_db):
    row = db.exec(
        select(MagnificationLookup).where(MagnificationLookup.magnification == mag)
    ).one()
    db.delete(row)
    db.commit()


@router.post("/visits/{visit_name}")
def register_client_to_visit(visit_name: str, client_info: ClientInfo, db=murfey_db):
    client_env = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_info.id)
    ).one()
    session = db.exec(select(Session).where(Session.id == client_env.session_id)).one()
    if client_env:
        client_env.visit = visit_name
        db.add(client_env)
        db.commit()
    if session:
        session.visit = visit_name
        db.add(session)
        db.commit()
    db.close()
    return client_info


@router.delete("/sessions/{session_id}/rsyncer")
def delete_rsyncer(session_id: int, source: Path, db=murfey_db):
    try:
        rsync_instance = db.exec(
            select(RsyncInstance)
            .where(RsyncInstance.session_id == session_id)
            .where(RsyncInstance.source == str(source))
        ).one()
        db.delete(rsync_instance)
        db.commit()
    except Exception:
        log.error(
            f"Failed to delete rsyncer for source directory {sanitise(str(source))!r} "
            f"in session {session_id}.",
            exc_info=True,
        )


@router.post("/sessions/{session_id}/rsyncer_stopped")
def register_stopped_rsyncer(
    session_id: int, rsyncer_source: RsyncerSource, db=murfey_db
):
    rsyncer = db.exec(
        select(RsyncInstance)
        .where(RsyncInstance.session_id == session_id)
        .where(RsyncInstance.source == rsyncer_source.source)
    ).one()
    rsyncer.transferring = False
    db.add(rsyncer)
    db.commit()


@router.post("/sessions/{session_id}/rsyncer_started")
def register_restarted_rsyncer(
    session_id: int, rsyncer_source: RsyncerSource, db=murfey_db
):
    rsyncer = db.exec(
        select(RsyncInstance)
        .where(RsyncInstance.session_id == session_id)
        .where(RsyncInstance.source == rsyncer_source.source)
    ).one()
    rsyncer.transferring = True
    db.add(rsyncer)
    db.commit()


@router.post("/visits/{visit_name}/increment_rsync_file_count")
def increment_rsync_file_count(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    rsync_instance = db.exec(
        select(RsyncInstance).where(
            RsyncInstance.source == rsyncer_info.source,
            RsyncInstance.destination == rsyncer_info.destination,
            RsyncInstance.session_id == rsyncer_info.session_id,
        )
    ).one()
    rsync_instance.files_counted += rsyncer_info.increment_count
    db.add(rsync_instance)
    db.commit()
    db.close()
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).inc(
        rsyncer_info.increment_count
    )
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).inc(
        rsyncer_info.increment_data_count
    )


@router.post("/visits/{visit_name}/increment_rsync_transferred_files")
def increment_rsync_transferred_files(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    rsync_instance = db.exec(
        select(RsyncInstance).where(
            RsyncInstance.source == rsyncer_info.source,
            RsyncInstance.destination == rsyncer_info.destination,
            RsyncInstance.session_id == rsyncer_info.session_id,
        )
    ).one()
    rsync_instance.files_transferred += rsyncer_info.increment_count
    db.add(rsync_instance)
    db.commit()
    db.close()


@router.post("/visits/{visit_name}/increment_rsync_transferred_files_prometheus")
def increment_rsync_transferred_files_prometheus(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    prom.transferred_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.increment_count)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.bytes)
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.increment_data_count)
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.data_bytes)


@router.post("/visits/{visit_name}/monitoring/{on}")
def change_monitoring_status(visit_name: str, on: int):
    prom.monitoring_switch.labels(visit=visit_name)
    prom.monitoring_switch.labels(visit=visit_name).set(on)


@router.get("/prometheus/{metric_name}")
def inspect_prometheus_metrics(
    metric_name: str,
):
    """
    A debugging endpoint that returns the current contents of any Prometheus
    gauges and counters that have been set up thus far.
    """

    # Extract the Prometheus metric defined in the Prometheus module
    metric: Optional[Counter | Gauge] = getattr(prom, metric_name, None)
    if metric is None or not isinstance(metric, (Counter, Gauge)):
        raise LookupError("No matching metric was found")

    # Package contents into dict and return
    results = {}
    if hasattr(metric, "_metrics"):
        for i, (label_tuple, sub_metric) in enumerate(metric._metrics.items()):
            labels = dict(zip(metric._labelnames, label_tuple))
            labels["value"] = sub_metric._value.get()
            results[i] = labels
        return results
    else:
        value = metric._value.get()
        return {"value": value}
