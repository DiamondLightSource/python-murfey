from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from murfey.workflows.fib.register_lamella_evaluation_image import run


def test_run(
    mocker: MockerFixture,
):
    mock_logger = mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.logger"
    )
    mock_murfey_db = MagicMock()
    message = {"dummy": "dummy"}
    run(message, mock_murfey_db)
    mock_logger.debug.assert_called_once()
