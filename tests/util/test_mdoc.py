from __future__ import annotations

from datetime import datetime
from pathlib import Path

from murfey.util.mdoc import get_block, get_global_data


def test_mdoc_file_parse_global_data():
    with open(Path(__file__).parent / "test.mdoc", "r") as md:
        data = get_global_data(md)
    assert len(data.keys()) == 6
    assert data["Voltage"] == "300"
    assert data["ImageSize"] == ("4096", "4096")
    assert data["PixelSpacing"] == "1.94"


def test_mdoc_file_parse_block():
    with open(Path(__file__).parent / "test.mdoc", "r") as md:
        data = get_block(md)
    assert len(data.keys()) == 32
    assert data["PixelSpacing"] == "1.94"
    assert data["TiltAngle"] == "-0.00949884"
    assert data["DateTime"] == datetime(2022, 8, 1, 18, 58, 35)


def test_mdoc_file_parse_multiple_blocks():
    with open(Path(__file__).parent / "test.mdoc", "r") as md:
        data = get_block(md)
        assert len(data.keys()) == 32
        assert data["PixelSpacing"] == "1.94"
        assert data["TiltAngle"] == "-0.00949884"
        assert data["DateTime"] == datetime(2022, 8, 1, 18, 58, 35)
        data = get_block(md)
        assert len(data.keys()) == 32
        assert data["PixelSpacing"] == "1.94"
        assert data["TiltAngle"] == "2.98863"
        assert data["DateTime"] == datetime(2022, 8, 1, 18, 59, 43)
