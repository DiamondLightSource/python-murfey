import os
os.environ["BEAMLINE"] = "m12"
print(os.environ["BEAMLINE"])
import main

def test_get_visit_info():
    print("BEAMLINE", os.environ["BEAMLINE"])
    response = main.get_visit_info("cm31095-1")
    assert response.status_code == 200

test_get_visit_info()