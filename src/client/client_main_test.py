import os
os.environ["MICROSCOPE"] = "m12"
print(os.environ["MICROSCOPE"])
import main

def test_get_visit_info():
    print("MICROSCOPE", os.environ["MICROSCOPE"])
    response = main.get_visit_info("cm31095-1")
    assert response.status_code == 200

test_get_visit_info()