from __future__ import annotations

import os

import main

os.environ["BEAMLINE"] = "m12"


def test_get_visit_info():
    response = main.get_visit_info("cm31111-1")  # Should be valid until end of 2021
    assert response.status_code == 200


test_get_visit_info()
