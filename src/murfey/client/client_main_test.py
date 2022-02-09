from __future__ import annotations

import os

os.environ["BEAMLINE"] = "m12"
print(os.environ["BEAMLINE"])
import main


def test_get_visit_info():
    print("BEAMLINE", os.environ["BEAMLINE"])
    response = main.get_visit_info("cm31111-1")  # Should be valid until end of 2021
    assert response.status_code == 200


test_get_visit_info()
