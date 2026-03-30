import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.workflows.fib.register_atlas import FIBAtlasMetadata, _parse_metadata, run

session_id = 10
visit_name = "cm12345-6"
instrument_name = "test_instrument"


@pytest.fixture
def visit_dir(tmp_path: Path):
    visit_dir = tmp_path / "data/2020" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


def create_electron_snapshot_metadata(
    voltage: float,
    shift_x: float,
    shift_y: float,
    len_x: float,
    len_y: float,
    pos_x: float,
    pos_y: float,
    pos_z: float,
    rotation: float,
    tilt_alpha: float,
    tilt_beta: float,
    pixels_x: int,
    pixels_y: int,
    pixel_size_x: float,
    pixel_size_y: float,
):
    # Create the XML Element structure present in the file
    root = ET.Element("Metadata")

    # ------
    # Optics
    # ------
    optics_node = ET.Element("Optics")

    voltage_node = ET.Element("AccelerationVoltage")
    voltage_node.text = str(voltage)
    optics_node.append(voltage_node)

    beam_shift_node = ET.Element("BeamShift")
    shift_x_node = ET.Element("X")
    shift_x_node.text = str(shift_x)
    beam_shift_node.append(shift_x_node)
    shift_y_node = ET.Element("Y")
    shift_y_node.text = str(shift_y)
    beam_shift_node.append(shift_y_node)
    optics_node.append(beam_shift_node)

    fov_node = ET.Element("ScanFieldOfView")
    len_x_node = ET.Element("X")
    len_x_node.text = str(len_x)
    fov_node.append(len_x_node)
    len_y_node = ET.Element("Y")
    len_y_node.text = str(len_y)
    fov_node.append(len_y_node)
    optics_node.append(fov_node)

    root.append(optics_node)

    # -------------
    # StageSettings
    # -------------
    stage_settings_node = ET.Element("StageSettings")
    # x, y, z
    stage_node = ET.Element("StagePosition")
    pos_x_node = ET.Element("X")
    pos_x_node.text = str(pos_x)
    stage_node.append(pos_x_node)
    pos_y_node = ET.Element("Y")
    pos_y_node.text = str(pos_y)
    stage_node.append(pos_y_node)
    pos_z_node = ET.Element("Z")
    pos_z_node.text = str(pos_z)
    stage_node.append(pos_z_node)
    rotation_node = ET.Element("Rotation")
    rotation_node.text = str(rotation)
    stage_node.append(rotation_node)
    # Angles
    tilt_node = ET.Element("Tilt")
    tilt_alpha_node = ET.Element("Alpha")
    tilt_alpha_node.text = str(tilt_alpha)
    tilt_node.append(tilt_alpha_node)
    tilt_beta_node = ET.Element("Beta")
    tilt_beta_node.text = str(tilt_beta)
    tilt_node.append(tilt_beta_node)
    stage_node.append(tilt_node)

    stage_settings_node.append(stage_node)
    root.append(stage_settings_node)

    # ------------
    # BinaryResult
    # ------------
    binary_result_node = ET.Element("BinaryResult")
    # ImageSize
    image_size_node = ET.Element("ImageSize")
    pixels_x_node = ET.Element("X")
    pixels_x_node.text = str(pixels_x)
    image_size_node.append(pixels_x_node)
    pixels_y_node = ET.Element("Y")
    pixels_y_node.text = str(pixels_y)
    image_size_node.append(pixels_y_node)
    binary_result_node.append(image_size_node)
    # PixelSize
    pixel_size_node = ET.Element("PixelSize")
    pixel_size_x_node = ET.Element("X")
    pixel_size_x_node.text = str(pixel_size_x)
    pixel_size_node.append(pixel_size_x_node)
    pixel_size_y_node = ET.Element("Y")
    pixel_size_y_node.text = str(pixel_size_y)
    pixel_size_node.append(pixel_size_y_node)
    binary_result_node.append(pixel_size_node)

    root.append(binary_result_node)

    xml_string = ET.tostring(root, encoding="unicode", xml_declaration=True)
    return xml_string


@pytest.mark.parametrize(
    "test_params",
    (
        (
            "Electron Snapshot",
            "some_project",
            2000,  # Voltage
            0,  # Beam shift X
            0,  # Y
            0.003072,  # Field of view X
            0.002048,  # Y
            0.003,  # Stage X
            0.0003,  # Y
            0.01,  # Z
            -1.309,  # Rotation
            0.8,  # Alpha tilt
            0,  # Beta tilt
            3072,  # Image size X
            2048,  # Y
            1e-6,  # Pixel size X
            1e-6,  # Y
        ),
        (
            "Electron Snapshot (2)",
            "another_project",
            2000,  # Voltage
            0,  # Beam shift X
            0,  # Y
            0.003072,  # Field of view X
            0.002048,  # Y
            -0.003,  # Stage X
            0.0003,  # Y
            0.01,  # Z
            1.309,  # Rotation
            0,  # Alpha tilt
            0,  # Beta tilt
            3072,  # Image size X
            2048,  # Y
            1e-6,  # Pixel size X
            1e-6,  # Y
        ),
    ),
)
def test_parse_metadata(
    mocker: MockerFixture,
    test_params: tuple[
        str,
        str,
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        int,
        int,
        float,
        float,
    ],
    visit_dir: Path,
):
    # Unpack test params
    (
        image_name,
        project_name,
        voltage,
        shift_x,
        shift_y,
        len_x,
        len_y,
        pos_x,
        pos_y,
        pos_z,
        rotation,
        tilt_alpha,
        tilt_beta,
        pixels_x,
        pixels_y,
        pixel_size_x,
        pixel_size_y,
    ) = test_params
    file = (
        visit_dir
        / "maps"
        / project_name
        / "LayersData/Layer"
        / image_name
        / f"{image_name}.tiff"
    )
    slot_number = 1 if pos_x < 0 else 2

    # Mock the results of opening an image file
    xml_string = create_electron_snapshot_metadata(
        voltage,
        shift_x,
        shift_y,
        len_x,
        len_y,
        pos_x,
        pos_y,
        pos_z,
        rotation,
        tilt_alpha,
        tilt_beta,
        pixels_x,
        pixels_y,
        pixel_size_x,
        pixel_size_y,
    )
    mock_image = MagicMock()
    mock_image.tag_v2 = {34683: xml_string}
    mocker.patch(
        "murfey.workflows.fib.register_atlas.PIL.Image.open",
        return_value=mock_image,
    )

    # Run the function and check that output is correct
    parsed = _parse_metadata(file, visit_name)

    assert parsed.visit_name == visit_name
    assert parsed.file == file
    assert parsed.voltage == voltage
    assert parsed.shift_x == shift_x
    assert parsed.shift_y == shift_y
    assert parsed.len_x == len_x
    assert parsed.len_y == len_y
    assert parsed.pos_x == pos_x
    assert parsed.pos_y == pos_y
    assert parsed.pos_z == pos_z
    assert parsed.rotation == rotation
    assert parsed.tilt_alpha == tilt_alpha
    assert parsed.tilt_beta == tilt_beta
    assert parsed.pixels_x == pixels_x
    assert parsed.pixels_y == pixels_y
    assert parsed.pixel_size_x == pixel_size_x
    assert parsed.pixel_size_y == pixel_size_y
    assert parsed.slot_number == slot_number
    assert parsed.site_name == f"{project_name}--slot_{slot_number}"
    assert parsed.pixel_size == 0.5 * (pixel_size_x + pixel_size_y)


def test_register_fib_imaging_site():
    pass


def test_run_with_db(
    mocker: MockerFixture,
    visit_dir: Path,
    murfey_db_session: Session,
):
    test_file = (
        visit_dir / "maps/LayersData/Layer/Electron Snapshot/Electron Snapshot.tiff"
    )

    # Add a test visit to the database
    if not (
        session_entry := murfey_db_session.exec(
            select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
        ).one_or_none()
    ):
        session_entry = MurfeyDB.Session(id=session_id)
    session_entry.name = visit_name
    session_entry.visit = visit_name
    session_entry.instrument_name = instrument_name

    murfey_db_session.add(session_entry)
    murfey_db_session.commit()

    # Mock the metadata returned from the image file
    mock_metadata = FIBAtlasMetadata(
        visit_name=visit_name,
        file=test_file,
        voltage=2000,
        shift_x=0,
        shift_y=0,
        len_x=0.003072,
        len_y=0.002048,
        pos_x=0.003,
        pos_y=0.0003,
        pos_z=0.01,
        rotation=-1.309,
        tilt_alpha=0.8,
        tilt_beta=0,
        pixels_x=3072,
        pixels_y=2048,
        pixel_size_x=1e-6,
        pixel_size_y=1e-6,
    )
    mocker.patch(
        "murfey.workflows.fib.register_atlas._parse_metadata",
        return_value=mock_metadata,
    )

    # Run the function and check that it's run through to completion
    assert run(
        session_id=session_id,
        file=test_file,
        murfey_db=murfey_db_session,
    )
