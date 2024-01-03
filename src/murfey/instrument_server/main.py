from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from murfey.instrument_server.api import router

log = logging.getLogger("murfey.instrument_server.main")

app = FastAPI(title="Murfey instrument server", debug=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
