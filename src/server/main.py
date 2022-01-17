from fastapi import FastAPI, Request, Response
from pydantic import BaseModel
import ispyb
from pprint import pprint
import sqlalchemy.exc
import sqlalchemy.orm
from ispyb.sqlalchemy import BLSession, Proposal
from typing import List, Optional
from requests import get

app = FastAPI()
db_session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(
        ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
    ))()

@app.get("/")
async def root(request: Request):
    client_host = request.client.host
    return {"client_host": client_host, "message": "Transfer Server"}

class Visits(BaseModel):
    start: str
    end: str
    beamline_name: str
    visit_name: str
    proposal_title: str

@app.get("/visits/{bl_name}")
async def visit_info(bl_name: str):
    query = db_session.query(BLSession).join(Proposal).filter(BLSession.proposalId == Proposal.proposalId, BLSession.beamLineName == bl_name).add_columns(BLSession.startDate, BLSession.endDate, BLSession.beamLineName, Proposal.proposalCode, Proposal.proposalNumber, BLSession.visit_number, Proposal.title).all()
    if query:
        return_query = [{"Start date": id.startDate, "End date": id.endDate, "Beamline name": id.beamLineName, "Visit name": id.proposalCode + str(id.proposalNumber) + '-' + str(id.visit_number), "Proposal title": id.title } for id in query]
        return return_query
    else:
        return None

@app.get("/pypi/{path}")
async def pypi_request(path: str):
    full_path = "https://pypi.org/simple/" + path
    full_path_response = get(full_path)
    return Response(content=full_path_response.content, media_type=full_path_response.headers['Content-Type'], status_code=200)

@app.get("/pypi/")
async def pypi_request():
    full_path = "https://pypi.org/simple/"
    full_path_response = get(full_path)
    return Response(content=full_path_response.content, media_type=full_path_response.headers['Content-Type'], status_code=200)

class File(BaseModel):
    name: str
    description: str
    size: int

@app.post("/files", response_model=File)
async def add_file(file: File):
    return file
