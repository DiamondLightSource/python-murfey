from __future__ import annotations

import os

os.environ["BEAMLINE"] = "m12"
import pytest

import murfey.client.main as main


@pytest.mark.xfail
def test_get_visit_info():
    response = main.get_visit_info("cm31111-1")  # Should be valid until end of 2022
    assert response.status_code == 200
