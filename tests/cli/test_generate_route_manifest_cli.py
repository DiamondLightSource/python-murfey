import sys

from pytest_mock import MockerFixture

from murfey.cli.generate_route_manifest import run


def test_run(
    mocker: MockerFixture,
):
    # Mock out print() and exit()
    mock_print = mocker.patch("builtins.print")
    mock_exit = mocker.patch("builtins.exit")

    # Run the function with its args
    sys.argv = ["", "--debug"]
    run()

    # Check that the final print message and exit() are called
    print_calls = mock_print.call_args_list
    last_print_call = print_calls[-1]
    last_printed = last_print_call.args[0]
    assert last_printed.startswith(
        "Route manifest for instrument and backend servers saved to"
    )
    mock_exit.assert_called_once()
