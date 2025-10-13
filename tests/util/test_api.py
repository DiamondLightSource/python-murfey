import pytest

from murfey.util.api import url_path_for

url_path_test_matrix: tuple[tuple[str, str, dict[str, str | int], str], ...] = (
    # Router name | Function name | kwargs | Expected URL
    (
        "instrument_server.api.router",
        "health",
        {},
        "/health",
    ),
    (
        "instrument_server.api.router",
        "stop_multigrid_watcher",
        {"session_id": 0, "label": "some_label"},
        "/sessions/0/multigrid_watcher/some_label",
    ),
    (
        "api.hub.router",
        "get_instrument_image",
        {"instrument_name": "test"},
        "/instrument/test/image",
    ),
    (
        "api.instrument.router",
        "check_if_session_is_active",
        {
            "instrument_name": "test",
            "session_id": 0,
        },
        "/instrument_server/instruments/test/sessions/0/active",
    ),
)


@pytest.mark.parametrize("test_params", url_path_test_matrix)
def test_url_path_for(test_params: tuple[str, str, dict[str, str | int], str]):
    # Unpack test params
    router_name, function_name, kwargs, expected_url_path = test_params
    assert (
        url_path_for(router_name=router_name, function_name=function_name, **kwargs)
        == expected_url_path
    )
