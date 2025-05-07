from ispyb.sqlalchemy import BLSession, Proposal
from sqlmodel import Session, select

from murfey.server.ispyb import get_session_id
from tests.conftest import ExampleVisit


def test_get_session_id(
    ispyb_db: Session,
):
    # Manually get the BLSession ID for comparison
    result = (
        ispyb_db.exec(
            select(BLSession)
            .join(Proposal)
            .where(BLSession.proposalId == Proposal.proposalId)
            .where(BLSession.beamLineName == ExampleVisit.instrument_name)
            .where(Proposal.proposalCode == ExampleVisit.proposal_code)
            .where(Proposal.proposalNumber == str(ExampleVisit.proposal_number))
            .where(BLSession.visit_number == ExampleVisit.visit_number)
        )
        .one()
        .sessionId
    )

    # Test function
    session_id = get_session_id(
        microscope="murfey",
        proposal_code="cm",
        proposal_number="12345",
        visit_number="6",
        db=ispyb_db,
    )
    assert result == session_id
