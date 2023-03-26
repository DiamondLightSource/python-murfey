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

        def set_elem(self, new_elem: str, values: dict):
            self.elem = new_elem

    dc = DummyContext()
    env.listeners["autoproc_program_ids"] = {dc.set_elem}
    assert len(env.listeners["autoproc_program_ids"]) == 1
    env.autoproc_program_ids = {"a": {"em-tomo-preprocess": 1}}
    assert dc.elem == "a"
    env.autoproc_program_ids = {
        "a": {"em-tomo-preprocess": 1},
        "b": {"em-tomo-preprocess": 2},
    }
    assert dc.elem == "b"


def test_murfey_instance_environment_write_to_json(env, tmp_path):
    env.write(base_path=tmp_path)
    assert (tmp_path / ".murfey_cache.json").exists()


def test_murfey_instance_environment_read_from_json(env, tmp_path):
    env.gain_ref = tmp_path / "gain.mrc"
    env.write(base_path=tmp_path)
    read_env = MurfeyInstanceEnvironment.read(
        urlparse("http://localhost:8000", allow_fragments=False),
        base_path=tmp_path,
    )
    assert read_env.gain_ref == tmp_path / "gain.mrc"
