from ispyb.sqlalchemy import BLSession, Person, Proposal

from murfey.server.ispyb import get_session_id


def test_get_session_id(
    ispyb_session,
):

    # Create some values to put into BLSession and Proposal
    # 'Person' is a required table
    person_db_entry = Person(
        login="murfey",
    )
    ispyb_session.add(person_db_entry)
    ispyb_session.commit()

    proposal_db_entry = Proposal(
        personId=Person.personId,
        proposalCode="cm",
        proposalNumber="12345",
    )
    ispyb_session.add(proposal_db_entry)
    ispyb_session.commit()

    bl_session_db_entry = BLSession(
        proposalId=proposal_db_entry.proposalId,
        beamLineName="murfey",
        visit_number=6,
    )
    ispyb_session.add(bl_session_db_entry)
    ispyb_session.commit()

    # Test function
    result = get_session_id(
        microscope="murfey",
        proposal_code="cm",
        proposal_number="12345",
        visit_number="6",
        db=ispyb_session,
    )
    assert result == bl_session_db_entry.sessionId
