from __future__ import annotations

from urllib.parse import urlparse

import pytest

from murfey.client.instance_environment import MurfeyInstanceEnvironment


@pytest.fixture
def env():
    return MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000", allow_fragments=False)
    )


def test_murfey_instance_environment_subscribe(env):
    assert env.url == urlparse("http://localhost:8000", allow_fragments=False)

    class DummyContext:
        elem = None

        def set_elem(self, new_elem: str):
            self.elem = new_elem

    dc = DummyContext()
    env.listeners["autoproc_program_ids"] = {dc.set_elem}
    assert len(env.listeners["autoproc_program_ids"]) == 1
    env.autoproc_program_ids = {"a": 1}
    assert dc.elem == "a"
    env.autoproc_program_ids = {"a": 1, "b": 2}
    assert dc.elem == "b"
