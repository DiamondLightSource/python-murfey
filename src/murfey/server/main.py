from __future__ import annotations

import logging

from backports.entry_points_selectable import entry_points
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app
from pydantic_settings import BaseSettings

import murfey.server
import murfey.server.api.auth
import murfey.server.api.bootstrap
import murfey.server.api.clem
import murfey.server.api.display
import murfey.server.api.file_io_frontend
import murfey.server.api.file_io_instrument
import murfey.server.api.hub
import murfey.server.api.instrument
import murfey.server.api.mag_table
import murfey.server.api.processing_parameters
import murfey.server.api.prometheus
import murfey.server.api.session_control
import murfey.server.api.session_info
import murfey.server.api.websocket
import murfey.server.api.workflow
from murfey.server import template_files
from murfey.util.config import get_security_config

log = logging.getLogger("murfey.server.main")

tags_metadata = [murfey.server.api.bootstrap.tag]


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


security_config = get_security_config()

settings = Settings()

app = FastAPI(title="Murfey server", debug=True, openapi_tags=tags_metadata)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=security_config.allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=template_files / "static"), name="static")
app.mount("/images", StaticFiles(directory=template_files / "images"), name="images")

# Add router endpoints to the API
app.include_router(murfey.server.api.bootstrap.version)
app.include_router(murfey.server.api.bootstrap.bootstrap)
app.include_router(murfey.server.api.bootstrap.cygwin)
app.include_router(murfey.server.api.bootstrap.msys2)
app.include_router(murfey.server.api.bootstrap.rust)
app.include_router(murfey.server.api.bootstrap.pypi)
app.include_router(murfey.server.api.bootstrap.plugins)

app.include_router(murfey.server.api.auth.router)

app.include_router(murfey.server.api.hub.router)
app.include_router(murfey.server.api.display.router)
app.include_router(murfey.server.api.processing_parameters.router)

app.include_router(murfey.server.api.file_io_frontend.router)
app.include_router(murfey.server.api.file_io_instrument.router)

app.include_router(murfey.server.api.instrument.router)

app.include_router(murfey.server.api.mag_table.router)

app.include_router(murfey.server.api.session_control.router)
app.include_router(murfey.server.api.session_control.spa_router)
app.include_router(murfey.server.api.session_control.tomo_router)

app.include_router(murfey.server.api.session_info.router)
app.include_router(murfey.server.api.session_info.correlative_router)
app.include_router(murfey.server.api.session_info.spa_router)
app.include_router(murfey.server.api.session_info.tomo_router)

app.include_router(murfey.server.api.workflow.router)
app.include_router(murfey.server.api.workflow.correlative_router)
app.include_router(murfey.server.api.workflow.spa_router)
app.include_router(murfey.server.api.workflow.tomo_router)
app.include_router(murfey.server.api.clem.router)

app.include_router(murfey.server.api.prometheus.router)

app.include_router(murfey.server.api.websocket.ws)

# Search external packages for additional routers to include in Murfey
for r in entry_points(group="murfey.routers"):
    app.include_router(r.load())
