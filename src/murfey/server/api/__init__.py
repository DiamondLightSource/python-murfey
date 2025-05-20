from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends
from prometheus_client import Counter, Gauge

import murfey.server.prometheus as prom
from murfey.server.api.auth import validate_token

log = logging.getLogger("murfey.server.api")

router = APIRouter(dependencies=[Depends(validate_token)])


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
