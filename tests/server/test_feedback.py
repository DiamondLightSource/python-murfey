from importlib.metadata import entry_points
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

feedback_callback_params_matrix = (
    # Murfey workflows currently present in pyproject.toml
    ("atlas_update",),
    ("clem.align_and_merge",),
    ("clem.process_raw_lifs",),
    ("clem.process_raw_tiffs",),
    ("clem.register_align_and_merge_result",),
    ("clem.register_preprocessing_result",),
    ("data_collection",),
    ("data_collection_group",),
    ("pato",),
    ("picked_particles",),
    ("picked_tomogram",),
    ("processing_job",),
    ("spa.flush_spa_preprocess",),
)


@pytest.mark.parametrize("test_params", feedback_callback_params_matrix)
def test_feedback_callback(
    mocker: MockerFixture,
    test_params: tuple[str],
):
    """
    Checks that feedback-callback loop works correctly for the entry points-based workflows
    """

    # Unpack test params
    (entry_point_name,) = test_params

    # Patch the functions used to generate the module-level variables
    mock_get_security_config = mocker.patch("murfey.util.config.get_security_config")
    mock_get_security_config.return_value = MagicMock()
    mock_url = mocker.patch("murfey.server.murfey_db.url")
    mock_url.return_value = MagicMock()
    mock_create_engine = mocker.patch("sqlmodel.create_engine")
    mock_create_engine.return_value = MagicMock()
    mock_murfey_db = MagicMock()
    mock_sql_session = mocker.patch("sqlmodel.Session")
    mock_sql_session.return_value = mock_murfey_db

    # Load the entry point and patch the executable it calls
    eps = list(entry_points(group="murfey.workflows", name=entry_point_name))
    assert len(eps) == 1  # Entry point should be present and unique
    mock_function = mocker.patch(eps[0].value.replace(":", "."))

    # Initialise after mocking
    from murfey.server.feedback import feedback_callback

    # Run the function and check that it calls the entry point correctly
    header = {"dummy": "dummy"}
    message = {"register": entry_point_name}
    feedback_callback(header, message, mock_murfey_db)
    mock_function.assert_called_once_with(message=message, murfey_db=mock_murfey_db)
