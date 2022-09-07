from __future__ import annotations

import itertools
import time
from pathlib import Path

import mrcfile
import numpy as np
import xmltodict

TILT = 1
tilt_angle = itertools.cycle(range(-60, 60, 10))


def initialise(dummy_location: Path) -> Path:
    base = dummy_location / "murfey_dummy"
    base.mkdir()
    microscope_dir = base / "M"
    detector_dir = base / "Data"
    microscope_dir.mkdir()
    detector_dir.mkdir()
    (detector_dir / "Supervisor").mkdir()
    return base


def tomo_file_name() -> str:
    angle = next(tilt_angle)
    global TILT
    if angle == -60 and TILT != 1:
        TILT += 1
    return f"Position_{TILT}_[{next(tilt_angle)}].mrc"


def write_mrc(base_path: Path, session_dir: str, mrc_name: str):
    rand_data = np.random.randint(0, high=128, size=(2048, 2048, 50), dtype=np.uint8)
    with mrcfile.new(base_path / "Data" / session_dir / mrc_name) as mrc:
        mrc.set_data(rand_data)


def write_xml(base_path: Path, session_dir: str, xml_name: str):
    xml_data = {
        "Acquisition": {
            "Info": {
                "ImageSize": {
                    "Width": 2048,
                    "Height": 2048,
                },
                "SensorPixelSize": {
                    "Width": 5e-11,
                    "Height": 5e-11,
                },
            }
        }
    }
    with open(base_path / "Data" / session_dir / xml_name, "w") as xml:
        xml.write(xmltodict.unparse(xml_data))


def generate_detector_data(base_path: Path, timeout: int | None = None):
    start_time = time.time()
    while True:
        try:
            if timeout is not None:
                if time.time() - start_time > timeout:
                    return
            tfn = tomo_file_name()
            write_mrc(base_path, "Supervisor", tfn)
            write_xml(base_path, "Supervisor", tfn.replace(".mrc", ".xml"))
            time.sleep(10)
        except KeyboardInterrupt:
            return
