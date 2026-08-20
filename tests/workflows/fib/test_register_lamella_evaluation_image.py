import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.util.config import MachineConfig
from murfey.workflows.fib.register_lamella_evaluation_image import (
    FIBImageMetadata,
    _parse_metadata,
    run,
)
from tests.conftest import ExampleVisit

session_id = 10
visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name


@pytest.fixture
def visit_dir(tmp_path: Path):
    visit_dir = tmp_path / "data/2020" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


def create_lamella_evaluation_image_metadata(
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
            "Metadata",  # Tag key
            "2026-04-15-21-50-14_drift_corrected_image_Finer Milling - Electron Image.png",
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
            -75,  # Rotation offset
            0.8,  # Alpha tilt
            0,  # Beta tilt
            2,  # Expected slot number
            3072,  # Image size X
            2048,  # Y
            1e-6,  # Pixel size X
            1e-6,  # Y
        ),
        (
            "Metadata",  # Tag key
            "2026-04-16-02-39-40_drift_corrected_image_Polishing 2 - Electron Image.png",
            "another_project",
            2000,  # Voltage
            0,  # Beam shift X
            0,  # Y
            0.003072,  # Field of view X
            0.002048,  # Y
            -0.003,  # Stage X
            0.0003,  # Y
            0.01,  # Z
            1.833,  # Rotation
            -75,  # Rotation offset
            0,  # Alpha tilt
            0,  # Beta tilt
            2,  # Expected slot number
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
        float,
        int,
        int,
        int,
        float,
        float,
    ],
    visit_dir: Path,
):
    # Unpack test params
    (
        tag_key,
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
        rotation_offset,
        tilt_alpha,
        tilt_beta,
        expected_slot_number,
        pixels_x,
        pixels_y,
        pixel_size_x,
        pixel_size_y,
    ) = test_params
    file = (
        visit_dir
        / "autotem"
        / project_name
        / "Sites"
        / "Lamella"
        / "LamellaEvaluationImages"
        / image_name
    )

    # Mock the results of opening an image file
    xml_string = create_lamella_evaluation_image_metadata(
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
    tags = dict.fromkeys(["Metadata", "MetadataAsINI"], 0)
    tags[tag_key] = xml_string
    mock_image = MagicMock(text=tags)
    mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.PIL.Image.open",
        return_value=mock_image,
    )

    # Run the function and check that output is correct
    parsed = _parse_metadata(file, visit_name, rotation_offset)

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
    assert parsed.slot_number == expected_slot_number
    assert parsed.site_name == f"{project_name}--slot_{expected_slot_number}"
    assert parsed.pixel_size == 0.5 * (pixel_size_x + pixel_size_y)


def test_run(
    mocker: MockerFixture,
    visit_dir: Path,
):
    # Set up parameters
    project_name = "some_project"

    # Mock the logger
    mock_logger = mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.logger"
    )

    # Mock the database call
    mock_session = MagicMock(visit_name=visit_name, instrument_name=instrument_name)
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec.return_value.one.return_value = mock_session

    # Mock the machine config
    machine_config = MachineConfig(
        calibrations={
            "rotation_offset": -75.0,
        }
    )
    mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.get_machine_config",
        return_value={instrument_name: machine_config},
    )

    # Create the test image file to use
    file = (
        visit_dir
        / "autotem"
        / project_name
        / "Sites"
        / "Lamella"
        / "LamellaEvaluationImages"
        / "2026-04-16-02-39-40_drift_corrected_image_Polishing 2 - Electron Image.png"
    )

    # Mock the results of '_parse_metadata'
    metadata = FIBImageMetadata(
        visit_name=visit_name,
        file=file,
        voltage=2000,
        shift_x=0,
        shift_y=0,
        len_x=0.003072,
        len_y=0.002048,
        pos_x=-0.003,
        pos_y=0.003,
        pos_z=0.01,
        rotation=1.833,
        slot_number=2,
        tilt_alpha=0,
        tilt_beta=0,
        pixels_x=3072,
        pixels_y=2048,
        pixel_size_x=1e-6,
        pixel_size_y=1e-6,
    )
    mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image._parse_metadata",
        return_value=metadata,
    )

    # Construct the message to pass to the function
    message = {
        "register": "fib.register_lamella_evaluation_image",
        "session_id": session_id,
        "lamella_image_file": str(file),
    }

    # Run function and check that expected calls were made
    result = run(message, mock_murfey_db)
    mock_logger.info.assert_called_with(
        "Extracted the following metadata from the image:\n",
        metadata.model_dump_json(indent=2),
    )
    assert result["success"]
