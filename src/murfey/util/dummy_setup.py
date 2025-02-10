from __future__ import annotations

import itertools
import time
from pathlib import Path

import mrcfile
import numpy as np
import xmltodict
import yaml
from rich.prompt import Confirm

TILT = 1
tilt_angle = itertools.cycle(range(-60, 70, 10))


def initialise(dummy_location: Path) -> Path:
    base = dummy_location / "murfey_dummy"
    base.mkdir()
    detector_dir = base / "Data"
    detector_dir.mkdir()
    (detector_dir / "Supervisor").mkdir()
    with open(base / "config.yaml", "w") as yaml_out:
        yaml.dump(
            {
                "m12": {
                    "acquisition_software": ["epu", "tomo", "serialem"],
                    "data_directories": [str(detector_dir)],
                    "rsync_basepath": str(dummy_location),
                    "calibrations": {"dummy": 0},
                }
            },
            yaml_out,
        )
        print(
            f"To set Murfey configuration for server: export MURFEY_MACHINE_CONFIGURATION={base / 'config.yaml'}"
        )
    return base


def tomo_file_name() -> str:
    angle = next(tilt_angle)
    global TILT
    file_name = f"Position_{TILT}_[{angle}].mrc"
    if angle == 60:
        TILT += 1
    return file_name


def write_mrc(
    base_path: Path,
    session_dir: str,
    mrc_name: str,
    data_dir: str = "Data",
    size: tuple = (1024, 1024, 50),
):
    rand_data = np.random.randint(0, high=128, size=size, dtype=np.uint8)
    with mrcfile.new(base_path / data_dir / session_dir / mrc_name) as mrc:
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


def generate_data(base_path: Path, timeout: int | None = None, pause: int = 10):
    if Confirm.ask("Begin simulated data acquisiton?"):
        start_time = time.time()
        while True:
            try:
                if timeout is not None:
                    if time.time() - start_time > timeout:
                        return
                tilt = TILT
                tfn = tomo_file_name()
                try:
                    write_mrc(
                        base_path,
                        "Supervisor",
                        f"Position_{tilt}.mrc",
                        data_dir="Data",
                        size=(128, 128),
                    )
                except ValueError:
                    pass
                write_mrc(base_path, "Supervisor", tfn)
                write_xml(base_path, "Supervisor", tfn.replace(".mrc", ".xml"))
                time.sleep(pause)
            except KeyboardInterrupt:
                return
