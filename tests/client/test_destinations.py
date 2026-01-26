from pathlib import Path
from unittest import mock

import pytest
from pytest_mock import MockerFixture

from murfey.client.destinations import (
    determine_default_destination,
    find_longest_data_directory,
)


@pytest.mark.parametrize(
    "test_params",
    (
        # File to match | Use Path? | Expected result
        (
            "X:/cm12345-6/Supervisor/Images-Disc1/file",
            True,
            ("X:/", "cm12345-6/Supervisor/Images-Disc1"),
        ),
        (
            "X:/DATA/cm12345-6/Supervisor/Images-Disc1/file",
            False,
            ("X:/DATA", "cm12345-6/Supervisor/Images-Disc1"),
        ),
        (
            "X:/DoseFractions/cm12345-6/Supervisor/Images-Disc1/file",
            True,
            ("X:/DoseFractions", "cm12345-6/Supervisor/Images-Disc1"),
        ),
        (
            "X:/DoseFractions/DATA/cm12345-6/Supervisor/Images-Disc1/file",
            False,
            ("X:/DoseFractions/DATA", "cm12345-6/Supervisor/Images-Disc1"),
        ),
    ),
)
def test_find_longest_data_directory(
    mocker: MockerFixture, test_params: tuple[str, bool, tuple[str, str]]
):
    # Unpack test params
    match_path, use_path, (expected_base_dir, expected_mid_dir) = test_params

    # Construct data directories using strings or Paths as needed
    data_directories: list[str] | list[Path] = [
        "X:",
        "X:/DATA",
        "X:/DoseFractions",
        "X:/DoseFractions/DATA",
    ]
    if use_path:
        data_directories = [Path(dd) for dd in data_directories]
    # Patch Pathlib's 'absolute()' function, since we are simulating Windows on Linux
    mocker.patch("murfey.client.destinations.Path.absolute", lambda self: self)

    base_dir, mid_dir = find_longest_data_directory(
        Path(match_path),
        data_directories,
    )
    assert base_dir == Path(expected_base_dir)
    assert mid_dir == Path(expected_mid_dir)


source_list = [
    ["X:/DoseFractions/cm12345-6/Supervisor", "Supervisor", True, "extra_name"],
    ["X:/DoseFractions/Supervisor/Images-Disc1", "Supervisor", False, ""],
    [
        "X:/DoseFractions/DATA/Supervisor/Sample1",
        "Supervisor_Sample1",
        False,
        "extra_name",
    ],
]


@mock.patch("murfey.client.destinations.capture_get")
@mock.patch("murfey.client.destinations.capture_post")
@pytest.mark.parametrize("sources", source_list)
def test_determine_default_destinations_suggested_path(mock_post, mock_get, sources):
    mock_environment = mock.Mock()
    mock_environment.murfey_session = 2
    mock_environment.instrument_name = "m01"
    mock_environment.destination_registry = {}

    source, source_name, touch, extra_directory = sources

    mock_get().json.return_value = {
        "data_directories": ["X:/DoseFractions", "X:/DoseFractions/DATA"]
    }
    mock_post().json.return_value = {"suggested_path": "/base_path/2025/cm12345-6/raw"}

    destination = determine_default_destination(
        visit="cm12345-6",
        source=Path(source),
        destination="2025",
        environment=mock_environment,
        token="token",
        touch=touch,
        extra_directory=extra_directory,
        use_suggested_path=True,
    )
    mock_get.assert_any_call(
        base_url=str(mock_environment.url.geturl()),
        router_name="session_control.router",
        function_name="machine_info_by_instrument",
        token="token",
        instrument_name="m01",
    )
    mock_post.assert_any_call(
        base_url=str(mock_environment.url.geturl()),
        router_name="file_io_instrument.router",
        function_name="suggest_path",
        token="token",
        visit_name="cm12345-6",
        session_id=2,
        data={
            "base_path": "2025/cm12345-6/raw",
            "touch": touch,
            "extra_directory": extra_directory,
        },
    )

    assert destination == f"/base_path/2025/cm12345-6/raw/{extra_directory}"
    assert (
        mock_environment.destination_registry.get(source_name)
        == "/base_path/2025/cm12345-6/raw"
    )


@mock.patch("murfey.client.destinations.capture_get")
@mock.patch("murfey.client.destinations.capture_post")
def test_determine_default_destinations_short_path(mock_post, mock_get):
    """Test for the case where the data directory is a drive"""
    mock_environment = mock.Mock()
    mock_environment.murfey_session = 2
    mock_environment.instrument_name = "m01"
    mock_environment.destination_registry = {}

    mock_get().json.return_value = {"data_directories": ["X:/"]}
    mock_post().json.return_value = {"suggested_path": "/base_path/2025/cm12345-6/raw"}

    destination = determine_default_destination(
        visit="cm12345-6",
        source=Path("X:/cm12345-6/Supervisor"),
        destination="2025",
        environment=mock_environment,
        token="token",
        touch=True,
        extra_directory="",
        use_suggested_path=True,
    )
    mock_get.assert_any_call(
        base_url=str(mock_environment.url.geturl()),
        router_name="session_control.router",
        function_name="machine_info_by_instrument",
        token="token",
        instrument_name="m01",
    )
    mock_post.assert_any_call(
        base_url=str(mock_environment.url.geturl()),
        router_name="file_io_instrument.router",
        function_name="suggest_path",
        token="token",
        visit_name="cm12345-6",
        session_id=2,
        data={
            "base_path": "2025/cm12345-6/raw",
            "touch": True,
            "extra_directory": "",
        },
    )

    assert destination == "/base_path/2025/cm12345-6/raw/"
    assert (
        mock_environment.destination_registry.get("Supervisor")
        == "/base_path/2025/cm12345-6/raw"
    )


@mock.patch("murfey.client.destinations.capture_get")
@pytest.mark.parametrize("sources", source_list)
def test_determine_default_destinations_skip_suggested(mock_get, sources):
    mock_environment = mock.Mock()
    mock_environment.murfey_session = 2
    mock_environment.instrument_name = "m01"
    mock_environment.destination_registry = {}

    source, source_name, touch, extra_directory = sources

    mock_get().json.return_value = {
        "data_directories": ["X:/DoseFractions", "X:/DoseFractions/DATA"]
    }

    destination = determine_default_destination(
        visit="cm12345-6",
        source=Path(source),
        destination="2025",
        environment=mock_environment,
        token="token",
        touch=touch,
        extra_directory=extra_directory,
        use_suggested_path=False,
    )
    mock_get.assert_any_call(
        base_url=str(mock_environment.url.geturl()),
        router_name="session_control.router",
        function_name="machine_info_by_instrument",
        token="token",
        instrument_name="m01",
    )

    assert destination == f"2025/cm12345-6/{Path(source).name}/{extra_directory}"


parameter_list_fail_cases = [
    ["X:/DoseFractions/cm12345-6", "", "2025", "X:/DoseFractions"],
    ["X:/DoseFractions/cm12345-6", "cm12345-6", "", "X:/DoseFractions"],
    ["X:/DoseFractions", "cm12345-6", "2025", "X:/DoseFractions"],
    ["X:/cm12345-6", "cm12345-6", "2025", "X:/DoseFractions"],
]


@mock.patch("murfey.client.destinations.capture_get")
@pytest.mark.parametrize("destination_params", parameter_list_fail_cases)
def test_determine_default_destinations_failures(mock_get, destination_params):
    """
    Test failure of the following cases:
    No visit, no destination, source = default, source not in default
    """
    mock_get().json.return_value = {
        "data_directories": ["X:/DoseFractions", "X:/DoseFractions/DATA"]
    }
    source, visit, destination, default_dests = destination_params
    mock_environment = mock.Mock()
    with pytest.raises(ValueError):
        determine_default_destination(
            visit=visit,
            source=Path(source),
            destination=destination,
            environment=mock_environment,
            token="token",
        )
