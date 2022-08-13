from __future__ import annotations

from urllib.parse import urlparse

import pytest

from murfey.client.instance_environment import MurfeyInstanceEnvironment


@pytest.fixture
def env():
    return MurfeyInstanceEnvironment(
        urlparse("http://localhost:8000", allow_fragments=False)
    )


def test_murfey_instance_environment_subscribe(env):
    class DummyContext:
        elem = None

        def set_elem(self, new_elem: str):
            self.elem = new_elem

    dc = DummyContext()
    env.subscribe(dc.set_elem)
    assert len(env._listeners) == 1
    env.register_app(1, "a")
    assert dc.elem == "a"
