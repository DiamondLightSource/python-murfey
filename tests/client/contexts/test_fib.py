import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from murfey.client.contexts.fib import FIBContext

# -------------------------------------------------------------------------------------
# FIBContext test utilty functions and fixtures
# -------------------------------------------------------------------------------------


def create_fib_maps_dataset_element(
    id: int,
    name: str,
    relative_path: str,
    center_x: float,
    center_y: float,
    center_z: float,
    size_x: float,
    size_y: float,
    size_z: float,
    rotation_angle: float,
    status: str,
):
    # Create dataset node
    dataset = ET.Element("Dataset")
    # ID node
    id_node = ET.Element("Id")
    id_node.text = str(id)
    dataset.append(id_node)

    # Name node
    name_node = ET.Element("Name")
    name_node.text = name
    dataset.append(name_node)

    # Stage position node
    box_center = ET.Element("BoxCenter")
    for tag, value in (
        ("CenterX", center_x),
        ("CenterY", center_y),
        ("CenterZ", center_z),
    ):
        node = ET.Element(tag)
        node.text = str(value)
        box_center.append(node)
    dataset.append(box_center)

    # Image size node
    box_size = ET.Element("BoxSize")
    for tag, value in (
        ("SizeX", size_x),
        ("SizeY", size_y),
        ("SizeZ", size_z),
    ):
        node = ET.Element(tag)
        node.text = str(value)
        box_size.append(node)
    dataset.append(box_size)

    # Rotation angle
    angle_node = ET.Element("RotationAngle")
    angle_node.text = str(rotation_angle)
    dataset.append(angle_node)

    # Relative path
    image_path_node = ET.Element("FinalImages")
    image_path_node.text = relative_path.replace("/", "\\")
    dataset.append(image_path_node)

    # Status
    status_node = ET.Element("Status")
    status_node.text = status
    dataset.append(status_node)

    return dataset


def create_fib_maps_xml_metadata(
    project_name: str,
    datasets: list[dict[str, Any]],
):
    # Create root node
    root = ET.Element("EMProject")

    # Project name node
    project_name_node = ET.Element("ProjectName")
    project_name_node.text = project_name
    root.append(project_name_node)

    # Datasets node
    datasets_node = ET.Element("Datasets")
    for id, dataset in enumerate(datasets):
        datasets_node.append(create_fib_maps_dataset_element(id, **dataset))
    root.append(datasets_node)

    return root


fib_maps_test_datasets = [
    {
        "name": name,
        "relative_path": relative_path,
        "center_x": cx,
        "center_y": cy,
        "center_z": cz,
        "size_x": sx,
        "size_y": sy,
        "size_z": sz,
        "rotation_angle": ra,
        "status": "Finished",
    }
    for (name, relative_path, cx, cy, cz, sx, sy, sz, ra) in (
        (
            "Electron Snapshot",
            "LayersData/Layer/Electron Snapshot",
            -0.002,
            -0.004,
            0.00000008,
            0.0036,
            0.0024,
            0.0,
            3.1415926535897931,
        ),
        (
            "Electron Snapshot (2)",
            "LayersData/Layer/Electron Snapshot (2)",
            -0.002,
            -0.004,
            0.00000008,
            0.0036,
            0.0024,
            0.0,
            3.1415926535897931,
        ),
        (
            "Electron Snapshot (3)",
            "LayersData/Layer/Electron Snapshot (3)",
            0.002,
            0.004,
            0.00000008,
            0.0036,
            0.0024,
            0.0,
            3.1415926535897931,
        ),
        (
            "Electron Snapshot (4)",
            "LayersData/Layer/Electron Snapshot (4)",
            0.002,
            0.004,
            0.00000008,
            0.0036,
            0.0024,
            0.0,
            3.1415926535897931,
        ),
    )
]


@pytest.fixture
def fib_maps_metadata_file(tmp_path: Path):
    metadata = create_fib_maps_xml_metadata(
        "test-project",
        fib_maps_test_datasets,
    )
    tree = ET.ElementTree(metadata)
    ET.indent(tree, space="  ")
    save_path = tmp_path / "EMproject.emxml"
    tree.write(save_path, encoding="utf-8")
    return save_path


@pytest.fixture
def fib_maps_images(tmp_path: Path):
    image_list = []
    for dataset in fib_maps_test_datasets:
        name = str(dataset["name"])
        relative_path = str(dataset["relative_path"])
        file = tmp_path / relative_path / f"{name}.tiff"
        if not file.exists():
            file.parent.mkdir(parents=True, exist_ok=True)
            file.touch()
        image_list.append(file)
    return image_list


# -------------------------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------------------------
def test_fib_autotem_context():
    pass


@pytest.mark.parametrize("metadata_first", ((False, True)))
def test_fib_maps_context(
    tmp_path: Path,
    fib_maps_metadata_file: Path,
    fib_maps_images: list[Path],
    metadata_first: bool,
):
    # Initialise the FIBContext
    basepath = tmp_path
    context = FIBContext(
        acquisition_software="maps",
        basepath=basepath,
        token="",
    )
    # Assert that its initial state is correct
    assert not context._electron_snapshots
    assert not context._electron_snapshot_metadata
    assert not context._electron_snapshots_submitted

    if metadata_first:
        # Read the metadata first
        context.post_transfer(fib_maps_metadata_file)
        # Metadata field should now be populated
        assert all(
            name in context._electron_snapshot_metadata.keys()
            for name in [image.stem for image in fib_maps_images]
        )
        # Parse the images one-by-one
        for image in fib_maps_images:
            name = image.stem
            context.post_transfer(image)
            # Entries should now start being removed from 'metadata' and 'images' fields
            assert (
                name not in context._electron_snapshots.keys()
                and name not in context._electron_snapshot_metadata.keys()
                and name in context._electron_snapshots_submitted
            )
    else:
        # Read in images first
        for image in fib_maps_images:
            name = image.stem
            context.post_transfer(image)
            assert (
                name in context._electron_snapshots.keys()
                and name not in context._electron_snapshot_metadata.keys()
                and name not in context._electron_snapshots_submitted
            )
        # Read in the metadata
        context.post_transfer(fib_maps_metadata_file)
        assert all(
            name in context._electron_snapshots_submitted
            and name not in context._electron_snapshots.keys()
            and name not in context._electron_snapshot_metadata.keys()
            for name in [file.stem for file in fib_maps_images]
        )


def test_fib_meteor_context():
    pass
