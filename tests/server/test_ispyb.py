from ispyb.sqlalchemy import BLSession, Proposal
from sqlalchemy import select
from sqlalchemy.orm import Session

from murfey.server.ispyb import get_session_id
from tests.conftest import ExampleVisit


def test_get_session_id(
    ispyb_db: Session,
):
    # Manually get the BLSession ID for comparison
    query = (
        ispyb_db.execute(
            select(BLSession)
            .join(Proposal)
            .where(BLSession.proposalId == Proposal.proposalId)
            .where(BLSession.beamLineName == ExampleVisit.instrument_name)
            .where(Proposal.proposalCode == ExampleVisit.proposal_code)
            .where(Proposal.proposalNumber == str(ExampleVisit.proposal_number))
            .where(BLSession.visit_number == ExampleVisit.visit_number)
        )
        .scalar_one()
        .sessionId
    )

    # Test function
    result = get_session_id(
        microscope=ExampleVisit.instrument_name,
        proposal_code=ExampleVisit.proposal_code,
        proposal_number=str(ExampleVisit.proposal_number),
        visit_number=str(ExampleVisit.visit_number),
        db=ispyb_db,
    )
    assert query == result
