from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.client.contexts.sim import SIMContext, _file_transferred_to, _get_source

visit_name = "cm12345-6"
instrument_name = "sim"
session_id = 1


@pytest.fixture
def visit_dir(tmp_path: Path):
    return tmp_path / visit_name


@pytest.fixture
def sim_data(visit_dir: Path):
    file_list = []
    for path in [
        "raw/SR002_G1/20260707_112417_SR002G1_F1F_BR",
        "raw/SR002_G1/20260707_112417_SR002G1_F1F_BFR",
        "raw/44drug_G2/20260703_114348_44drug_G2_E2DR_GR",
        "raw/44drug_G2/20260703_113142_44drug_G2_E2DR_GFR",
        "raw/SR002_G1/20260707_112417_SR002G1_F1F_BR_FL",
        "raw/SR002_G1/20260707_112417_SR002G1_F1F_BFR_FL",
        "raw/44drug_G2/20260703_114348_44drug_G2_E2DR_GR_FL",
        "raw/44drug_G2/20260703_113142_44drug_G2_E2DR_GFR_FL",
    ]:
        file = visit_dir / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch(exist_ok=True)
        file_list.append(file)
    return file_list


def test_get_source(
    tmp_path: Path,
    visit_dir: Path,
    sim_data: list[Path],
):
    # Mock the MurfeyInstanceEnvironment
    mock_environment = MagicMock()
    mock_environment.sources = [
        visit_dir,
        tmp_path / "another_dir",
    ]
    # Check that the correct source directory is found
    for file in sim_data:
        assert _get_source(file, mock_environment) == visit_dir


def test_file_transferred_to(
    tmp_path: Path,
    visit_dir: Path,
    sim_data: list[Path],
):
    # Mock the environment
    mock_environment = MagicMock()
    mock_environment.default_destinations = {visit_dir: "current_year"}
    mock_environment.visit = visit_name

    # Iterate across the FIB files to compare against
    destination_dir = tmp_path / "sim" / "data" / "current_year" / visit_name
    for file in sim_data:
        # Work out what the expected destination will be
        assert _file_transferred_to(
            environment=mock_environment,
            source=visit_dir,
            file_path=file,
            rsync_basepath=tmp_path / "sim" / "data",
        ) == destination_dir / file.relative_to(visit_dir)


def test_sim_context_initialises(tmp_path: Path):
    # Initialise the context with dummy variables
    base_path = tmp_path
    machine_config = {"dummy": "dummy"}
    context = SIMContext(
        "sim",
        basepath=base_path,
        machine_config=machine_config,
        token="dummy",
    )

    assert context._basepath == base_path
    assert context._machine_config == machine_config
    assert context._token == "dummy"
    assert context.name == "SIMContext"


@pytest.mark.parametrize(
    "test_params",
    (  # Has environment | Has source | Has destination
        # Success case
        (True, True, True),
        # Fail cases
        (True, True, False),  # No destination
        (True, False, True),  # No source
        (False, True, True),  # No environment
    ),
)
def test_post_transfer(
    mocker: MockerFixture,
    test_params: tuple[bool, bool, bool],
    tmp_path: Path,
    visit_dir: Path,
    sim_data: list[Path],
):
    # Unpack test params
    use_env, has_src, has_dst = test_params

    # Mock the environment
    mock_environment = None
    if use_env:
        mock_environment = MagicMock()
        mock_environment.visit = visit_name
        mock_environment.instrument_name = instrument_name
        mock_environment.murfey_session = session_id

    # Mock the logger to check if specific logs are triggered
    mock_logger = mocker.patch("murfey.client.contexts.sim.logger")

    # Iterate across the FIB files to compare against
    destination_dir = tmp_path / "sim" / "data" / "current_year" / visit_name
    destination_files = [
        destination_dir / file.relative_to(visit_dir) for file in sim_data
    ]

    # Mock the functions used in 'post_transfer'
    mock_get_source = mocker.patch("murfey.client.contexts.sim._get_source")
    mock_get_source.return_value = tmp_path if has_src else None

    mock_file_transferred_to = mocker.patch(
        "murfey.client.contexts.sim._file_transferred_to"
    )
    if has_dst:
        mock_file_transferred_to.side_effect = destination_files
    else:
        mock_file_transferred_to.return_value = None

    mock_capture_post = mocker.patch("murfey.client.contexts.sim.capture_post")

    # Initialise the SIMContext
    basepath = tmp_path
    context = SIMContext(
        acquisition_software="sim",
        basepath=basepath,
        machine_config={},
        token="dummy",
    )
    for file in sim_data:
        context.post_transfer(file, environment=mock_environment)
    if not use_env:
        mock_logger.warning.assert_called_with("No environment passed in")
    elif not has_src:
        mock_logger.warning.assert_called_with(f"No source found for file {file}")
    elif not has_dst:
        mock_logger.warning.assert_called_with(
            f"Could not find destination file path for {file.name!r}"
        )
    else:
        mock_get_source.assert_called_with(file, mock_environment)
        mock_file_transferred_to.assert_called_with(
            environment=mock_environment,
            source=basepath,
            file_path=file,
            rsync_basepath=Path(""),
        )

        assert mock_capture_post.call_count == len(sim_data)
        for dst in destination_files:
            mock_capture_post.assert_any_call(
                base_url=mock.ANY,
                router_name="workflow_sim.router",
                function_name="request_sim_processing",
                token=context._token,
                instrument_name=instrument_name,
                data={
                    "file": f"{dst}",
                },
                # Endpoint kwargs
                session_id=session_id,
            )
