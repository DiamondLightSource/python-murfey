from ispyb.sqlalchemy import BLSession, Proposal
from pytest import mark
from sqlalchemy import select
from sqlalchemy.orm import Session

from murfey.server.ispyb import get_proposal_id, get_session_id
from tests.conftest import ExampleVisit


def test_get_session_id(
    ispyb_db_session: Session,
):
    # Manually get the BLSession ID for comparison
    bl_session_id = (
        ispyb_db_session.execute(
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
        db=ispyb_db_session,
    )
    assert bl_session_id == result


def test_get_proposal_id(
    ispyb_db_session: Session,
):
    # Manually query the Proposal ID
    proposal_id = (
        ispyb_db_session.execute(
            select(Proposal)
            .where(Proposal.proposalCode == ExampleVisit.proposal_code)
            .where(Proposal.proposalNumber == str(ExampleVisit.proposal_number))
        )
        .scalar_one()
        .proposalId
    )

    # Test function
    result = get_proposal_id(
        proposal_code=ExampleVisit.proposal_code,
        proposal_number=str(ExampleVisit.proposal_number),
        db=ispyb_db_session,
    )
    assert proposal_id == result


@mark.skip
def test_get_sub_samples_from_visit():
    pass


@mark.skip
def test_get_all_ongoing_visits():
    pass
