from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.session_info import gather_upstream_files
from murfey.util.models import UpstreamFileRequestInfo


@pytest.mark.parametrize(
    "search_strings",
    (
        ["dummy"],
        [],
        None,
    ),
)
@pytest.mark.asyncio
async def test_gather_upstream_files(
    mocker: MockerFixture,
    tmp_path: Path,
    search_strings: list[str] | None,
):
    # Construct dictionary to pass to Pydantic model
    session_id = 1
    upstream_instrument = "dummy"
    upstream_visit_path = str(tmp_path / "dummy")
    params_dict: dict[str, Any] = {
        "upstream_instrument": upstream_instrument,
        "upstream_visit_path": upstream_visit_path,
    }
    if search_strings is not None:
        params_dict["search_strings"] = search_strings

    # Validate the incoming message
    params = UpstreamFileRequestInfo(**params_dict)

    # Patch the actual 'gather_upstream_files' function
    mock_gather = mocker.patch("murfey.server.api.session_info._gather_upstream_files")

    # Create a mock database session
    mock_db = MagicMock()

    # Run the function and check that the expected calls were made:
    await gather_upstream_files(
        visit_name="dummy",
        session_id=session_id,
        upstream_file_request=params,
        db=mock_db,
    )
    mock_gather.assert_called_with(
        session_id=session_id,
        upstream_instrument=upstream_instrument,
        upstream_visit_path=Path(upstream_visit_path),
        search_strings=search_strings,
        db=mock_db,
    )
