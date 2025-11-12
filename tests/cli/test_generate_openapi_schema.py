import sys
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

import murfey
from murfey.cli.generate_openapi_schema import run

params_matrix: tuple[tuple[str | None, str | None, bool], ...] = (
    # Target | Output | To File
    (None, None, False),
    ("instrument-server", "json", True),
    ("server", "yaml", False),
    ("instrument-server", "yaml", False),
    ("server", "json", True),
)


@pytest.mark.parametrize("test_params", params_matrix)
def test_run(
    mocker: MockerFixture,
    tmp_path: Path,
    test_params: tuple[str | None, str | None, bool],
):
    # Unpack test params
    target, output, to_file = test_params

    # Mock out print() and exit()
    mock_print = mocker.patch("builtins.print")
    mock_exit = mocker.patch("builtins.exit")

    # Construct the CLI args
    sys_args = [""]
    if target is not None:
        sys_args.extend(["-t", target])
    if output is not None:
        sys_args.extend(["-o", output])

    target = target if target is not None else "server"
    output = output if output is not None else "yaml"
    if to_file:
        save_path = tmp_path / f"openapi.{output}"
        sys_args.extend(["-f", str(save_path)])
    else:
        save_path = Path(murfey.__path__[0]) / "util" / f"openapi-{target}.{output}"
    sys_args.extend(["--debug"])
    sys.argv = sys_args

    # Run the function and check that it runs as expected
    run()
    print_calls = mock_print.call_args_list
    last_print_call = print_calls[-1]
    last_printed = last_print_call.args[0]
    assert last_printed.startswith("OpenAPI schema saved to")
    mock_exit.assert_called_once()
    assert save_path.exists()


failure_params_matrix = (
    ["-t", "blah"],
    ["-o", "blah"],
)


@pytest.mark.parametrize("test_params", failure_params_matrix)
def test_run_fails(test_params: list[str]):
    # Construct the CLI args
    sys_args = [""]
    sys_args.extend(test_params)
    sys.argv = sys_args

    with pytest.raises(ValueError):
        run()
