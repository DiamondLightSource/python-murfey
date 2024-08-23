from __future__ import annotations

import logging
import os

import importlib_metadata
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app
from pydantic import BaseSettings

import murfey.server
import murfey.server.api.auth
import murfey.server.api.bootstrap
import murfey.server.api.clem
import murfey.server.api.spa
import murfey.server.websocket
import murfey.util.models
from murfey.server import template_files

if os.getenv("MURFEY_DEMO"):
    from murfey.server.demo_api import router
else:
    from murfey.server.api import router


log = logging.getLogger("murfey.server.main")

tags_metadata = [murfey.server.api.bootstrap.tag]


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


settings = Settings()

app = FastAPI(title="Murfey server", debug=True, openapi_tags=tags_metadata)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=template_files / "static"), name="static")
app.mount("/images", StaticFiles(directory=template_files / "images"), name="images")

# Add router endpoints to the API
app.include_router(router)
app.include_router(murfey.server.api.bootstrap.bootstrap)
app.include_router(murfey.server.api.bootstrap.cygwin)
app.include_router(murfey.server.api.bootstrap.pypi)
app.include_router(murfey.server.api.bootstrap.plugins)
app.include_router(murfey.server.api.bootstrap.version)
app.include_router(murfey.server.api.clem.router)
app.include_router(murfey.server.api.spa.router)
app.include_router(murfey.server.api.auth.router)
app.include_router(murfey.server.websocket.ws)

for r in importlib_metadata.entry_points(group="murfey.routers"):
    app.include_router(r.load())
