import xml.etree.ElementTree as ET
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.client.contexts.fib import (
    MILLING_STEP_NAMES,
    STAGE_POSITION_NAMES,
    STAGE_POSITION_VALUES,
    FIBContext,
    FIBImage,
    _file_transferred_to,
    _get_source,
    _parse_boolean,
)
from murfey.util.models import LamellaSiteInfo

# Mock session values
num_lamellae = 5
visit_name = "cm12345-6"
project_name = visit_name.replace("-", "_")


# -------------------------------------------------------------------------------------
# FIBContext test utilty functions and fixtures
# -------------------------------------------------------------------------------------


def _create_milling_steps():
    # Create a dict with the milling steps sorted by the recipe
    milling_steps: dict[str, list[str]] = {}
    for key in MILLING_STEP_NAMES.keys():
        recipe, step = [s.strip() for s in key.split(" - ", 1)]
        if not milling_steps.get(recipe, []):
            milling_steps[recipe] = [step]
        else:
            milling_steps[recipe].append(step)
    return milling_steps


milling_steps: dict[str, list[str]] = _create_milling_steps()

# Test values to insert into the mock metadata
stage_values = {
    "x": "3.15911143012073 mm",
    "y": "-0.627002440038438 mm",
    "z": "32.0781899453239 mm",
    "rotation": "284.999355310423 °",
    "tilt_alpha": "44.998223254214 °",
}


@pytest.fixture
def visit_dir(tmp_path: Path):
    return tmp_path / visit_name


def _create_stage_position_node(stage_values: dict[str, str]):
    stage_position_node = ET.Element("StagePosition")
    for key, value in stage_values.items():
        node = ET.Element(STAGE_POSITION_VALUES[key])
        node.text = value
        stage_position_node.append(node)
    return stage_position_node


def _create_activity_node(
    step: str,
    recipe: str,
    has_activity_name: bool = True,
):
    activity_node = ET.Element("Activity")
    if has_activity_name:
        activity_name_node = ET.Element("Name")
        activity_name_node.text = step
        activity_node.append(activity_name_node)

    # Add common nodes
    # Is step enabled?
    enabled_node = ET.Element("IsEnabled")
    enabled_node.text = "true"
    activity_node.append(enabled_node)

    # Execution result
    activity_metadata_node = ET.Element("ActivityMetadata")
    execution_result_node = ET.Element("ExecutionResult")
    execution_result_node.text = "Finished"
    activity_metadata_node.append(execution_result_node)
    activity_node.append(activity_metadata_node)

    # Execution time
    execution_time_node = ET.Element("ExecutionTime")
    execution_time_node.text = "200 s"

    # Add activity-sepcific nodes
    # Activities with "MillingAngle" node
    if step == "Milling Angle":
        milling_angle_node = ET.Element("MillingAngle")
        milling_angle_node.text = "12.0 °"
        activity_node.append(milling_angle_node)
    # Activities with "SiteLocationType" node
    if step in (
        "Image Acquisition",
        "Reference Definition",
        "Reference Redefinition 1",
        "Reference Redefinition 2",
        "Rough Milling - Electron Image",
        "Medium Milling - Electron Image",
        "Fine Milling - Electron Image",
        "Finer Milling - Electron Image",
        "Polishing 1 - Electron Image",
        "Polishing 2 - Ion Image",
        "Polishing 2 - Electron Image",
    ):
        site_location_type_node = ET.Element("SiteLocationType")
        site_location_type_node.text = "Chunk" if recipe == "Milling" else "Thinning"
        activity_node.append(site_location_type_node)
    # Nodes with beam information
    if step in (
        "Artificial Features",
        "Stress Relief Cuts",
        "Rough Milling",
        "Rough Milling - Electron Image",
        "Medium Milling",
        "Medium Milling - Electron Image",
        "Fine Milling",
        "Fine Milling - Electron Image",
        "Finer Milling",
        "Finer Milling - Electron Image",
        "Polishing 1",
        "Polishing 1 - Electron Image",
        "Polishing 2",
        "Polishing 2 - Ion Image",
        "Polishing 2 - Electron Image",
    ):
        # BeamPreset parent node
        beam_node_name = "BeamPreset"
        if "image" not in step.lower():
            beam_node_name = "MillingPreset"
        beam_node = ET.Element(beam_node_name)

        # Use different values for ion and electron images
        beam_type = "Electron"
        voltage = "2 kV"
        current = "25 pA"
        if "ion" in step.lower() or "image" not in step.lower():
            beam_type = "Ion"
            voltage = "30 kV"
            current = "30 pA"

        beam_type_node = ET.Element("BeamType")
        beam_type_node.text = beam_type
        beam_node.append(beam_type_node)

        voltage_node = ET.Element("HighVoltage")
        voltage_node.text = voltage
        beam_node.append(voltage_node)

        current_node = ET.Element("BeamCurrent")
        current_node.text = current
        beam_node.append(current_node)

        activity_node.append(beam_node)

    # Nodes with milling information
    if step in (
        "Stress Relief Cuts",
        "Rough Milling",
        "Medium Milling",
        "Fine Milling",
        "Finer Milling",
        "Polishing 1",
        "Polishing 2",
    ):
        # All 7 have DepthCorrection node
        depth_correction_node = ET.Element("DepthCorrection")
        depth_correction_node.text = "3"
        activity_node.append(depth_correction_node)

        # "Rough Milling" has TrenchHeight nodes
        if step == "Rough Milling":
            trench_height_front_node = ET.Element("FrontTrenchHeight")
            trench_height_front_node.text = "2 μm"
            activity_node.append(trench_height_front_node)

            trench_height_rear_node = ET.Element("RearTrenchHeight")
            trench_height_rear_node.text = "8 μm"
            activity_node.append(trench_height_rear_node)

        # "Stress Relief Cuts" does not have other fields
        if step != "Stress Relief Cuts":
            # OffsetFromLamella node
            lamella_offset_node = ET.Element("OffsetFromLamella")
            lamella_offset_node.text = "2 μm"
            activity_node.append(lamella_offset_node)

            # LamellaFrontLeftWidthOverlap node
            width_overlap_front_left_node = ET.Element("LamellaFrontLeftWidthOverlap")
            width_overlap_front_left_node.text = "2 μm"
            activity_node.append(width_overlap_front_left_node)

            # LamellaFrontRightWidthOverlap node
            width_overlap_front_right_node = ET.Element("LamellaFrontRightWidthOverlap")
            width_overlap_front_right_node.text = "2 μm"
            activity_node.append(width_overlap_front_right_node)

            # LamellaRearLeftWidthOverlap node
            width_overlap_rear_left_node = ET.Element("LamellaRearLeftWidthOverlap")
            width_overlap_rear_left_node.text = "2 μm"
            activity_node.append(width_overlap_rear_left_node)

            # LamellaRearRightWidthOverlap node
            width_overlap_rear_right_node = ET.Element("LamellaRearRightWidthOverlap")
            width_overlap_rear_right_node.text = "2 μm"
            activity_node.append(width_overlap_rear_right_node)

    return activity_node


def _create_site_node(
    site_prefix: str,
    site_num: int,
    has_site_name: bool = True,
    has_recipes: bool = True,
    has_recipe_name: bool = True,
    has_activities: bool = True,
    has_activity_name: bool = True,
):
    # Create the root Site node
    site_node = ET.Element("Site")

    if has_site_name:
        name_node = ET.Element("Name")
        name_node.text = site_prefix
        # If the site name starts with "Lamella"
        if site_prefix == "Lamella":
            if site_num > 1:
                name_node.text += f" ({site_num})"
        # If the site name starts with "Site"
        else:
            name_node.text += f" #{site_num}"
        site_node.append(name_node)

    # Create the stage position nodes
    parameters_node = ET.Element("Parameters")
    for path in STAGE_POSITION_NAMES.values():
        inner_node: ET.Element | None = None
        for n, part in enumerate(reversed(path.split("/"))):
            # Create the stage position node
            match part:
                # Create the innermost StagePosition node
                case "StagePosition" if n == 0:
                    inner_node = _create_stage_position_node(
                        stage_values=stage_values,
                    )
                # Append more than one inner node to Parameters node
                case "Parameters":
                    if inner_node is not None:
                        parameters_node.append(inner_node)
                # Append every other inner node to a new node
                case _:
                    if inner_node is not None:
                        node = ET.Element(part)
                        node.append(inner_node)
                        inner_node = node
        if inner_node is not None:
            site_node.append(inner_node)
    # Append Parameters node separately
    site_node.append(parameters_node)

    # Create the recipe and activity nodes
    workflow_node = ET.Element("Workflow")
    if has_recipes:
        for recipe, steps in milling_steps.items():
            # Create a Recipe node
            recipe_node = ET.Element("Recipe")
            if has_recipe_name:
                recipe_name_node = ET.Element("Name")
                recipe_name_node.text = recipe
                recipe_node.append(recipe_name_node)

            # Iterate and create Activity nodes
            if has_activities:
                activities_node = ET.Element("Activities")
                for step in steps:
                    activities_node.append(
                        _create_activity_node(
                            step,
                            recipe,
                            has_activity_name=has_activity_name,
                        )
                    )
                recipe_node.append(activities_node)

            workflow_node.append(recipe_node)
    site_node.append(workflow_node)
    return site_node


def create_fib_autotem_project_data(
    visit_dir: Path,
    project_name: str,
    site_prefix: str,
    has_project_name: bool = True,
    has_sites: bool = True,
    has_site_name: bool = True,
    has_recipes: bool = True,
    has_recipe_name: bool = True,
    has_activities: bool = True,
    has_activity_name: bool = True,
):
    # Create root structure
    autotem_node = ET.Element("AutoTEM")
    project_node = ET.Element("Project", {"Origin": "MAPS"})

    if has_project_name:
        project_name_node = ET.Element("Name")
        project_name_node.text = project_name
        project_node.append(project_name_node)

    site_parent_node = ET.Element("Sites")
    if has_sites:
        # Construct individual Site nodes
        for n in reversed(range(num_lamellae)):
            n += 1
            site_parent_node.append(
                _create_site_node(
                    site_prefix,
                    n,
                    has_site_name=has_site_name,
                    has_recipes=has_recipes,
                    has_recipe_name=has_recipe_name,
                    has_activities=has_activities,
                    has_activity_name=has_activity_name,
                )
            )

    project_node.append(site_parent_node)
    autotem_node.append(project_node)

    # Save the mock XML file
    file = visit_dir / f"autotem/{project_name}/ProjectData.dat"
    file.parent.mkdir(parents=True, exist_ok=True)
    tree = ET.ElementTree(autotem_node)
    ET.indent(tree, space="  ")
    tree.write(file, encoding="utf-8", xml_declaration=True)
    return file


@pytest.fixture
def fib_autotem_dc_images(visit_dir: Path):
    stages = ("Rough-Milling", "Polishing-1", "Polishing-2")
    images_per_stage = 2

    # Create images as Murfey would expect to find them in the DCImages folder
    image_list = []
    for l in range(num_lamellae):
        for s, stage in enumerate(stages):
            for i in range(images_per_stage):
                lamella_folder = "Lamella"
                if l > 0:
                    lamella_folder += f" ({l + 1})"
                # Continuously increment seconds count between files
                timestamp = (
                    f"2025-05-10-12-34-{str(0 + i + (s * images_per_stage)).zfill(2)}"
                )
                file = (
                    visit_dir
                    / f"autotem/visit/Sites/{lamella_folder}"
                    / "DCImages/DCM_2025-05-10-12-34-00.125"
                    / f"{timestamp}-{stage}-dc_rescan-image-.png"
                )
                if not file.exists():
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.touch()
                image_list.append(file)
    return image_list


@pytest.fixture
def fib_maps_images(visit_dir: Path):
    image_list = []
    for i in range(4):
        name = "Electron Snapshot"
        if i > 0:
            name += f" ({i + 1})"
        file = visit_dir / f"maps/{project_name}/LayersData/Layer" / f"{name}.tiff"
        if not file.exists():
            file.parent.mkdir(parents=True, exist_ok=True)
            file.touch()
        image_list.append(file)
    return image_list


# -------------------------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "test_params",
    (  # Input | Expected output
        ("True", True),
        ("true", True),
        ("T", True),
        ("t", True),
        ("1", True),
        ("False", False),
        ("false", False),
        ("F", False),
        ("f", False),
        ("0", False),
    ),
)
def test_parse_boolean(test_params: tuple[str, bool]):
    text, expected_result = test_params
    assert _parse_boolean(text) == expected_result


def test_get_source(
    tmp_path: Path,
    visit_dir: Path,
    fib_maps_images: list[Path],
):
    # Mock the MurfeyInstanceEnvironment
    mock_environment = MagicMock()
    mock_environment.sources = [
        visit_dir,
        tmp_path / "another_dir",
    ]
    # Check that the correct source directory is found
    for file in fib_maps_images:
        assert _get_source(file, mock_environment) == visit_dir


def test_file_transferred_to(
    tmp_path: Path,
    visit_dir: Path,
    fib_maps_images: list[Path],
):
    # Mock the environment
    mock_environment = MagicMock()
    mock_environment.default_destinations = {visit_dir: "current_year"}
    mock_environment.visit = visit_name

    # Iterate across the FIB files to compare against
    destination_dir = tmp_path / "fib" / "data" / "current_year" / visit_name
    for file in fib_maps_images:
        # Work out what the expected destination will be
        assert _file_transferred_to(
            environment=mock_environment,
            source=visit_dir,
            file_path=file,
            rsync_basepath=tmp_path / "fib" / "data",
        ) == destination_dir / file.relative_to(visit_dir)


@pytest.mark.parametrize(
    "test_params",
    (
        # Pass cases
        (True, True, True, True, True, True, True, True),  # DC images
        (True, True, True, True, True, True, True, False),  # No DC images
        # Only one of these, and the last one, should be False at a given time
        (True, True, True, True, True, True, False, False),  # No activity name
        (True, True, True, True, True, False, True, False),  # No activity content
        (True, True, True, True, False, True, True, False),  # No recipe name
        (True, True, True, False, True, True, True, False),  # No recipe content
        (True, True, False, True, True, True, True, False),  # No site name
        (True, False, True, True, True, True, True, False),  # No site contents
        (False, True, True, True, True, True, True, False),  # No project name
    ),
)
def test_handle_autotem_metadata(
    mocker: MockerFixture,
    test_params: tuple[bool, bool, bool, bool, bool, bool, bool, bool],
    tmp_path: Path,
    visit_dir: Path,
):
    # Unpack test params
    (
        has_project_name,
        has_sites,
        has_site_name,
        has_recipes,
        has_recipe_name,
        has_activities,
        has_activity_name,
        has_drift_correction_images,
    ) = test_params

    # Mock the environment
    mock_environment = MagicMock()
    mock_environment.visit = visit_name

    # Mock the logger to check that specific logs are called
    mock_logger = mocker.patch("murfey.client.contexts.fib.logger")

    # Mock '_get_source'
    mock_get_source = mocker.patch("murfey.client.contexts.fib._get_source")
    mock_get_source.return_value = tmp_path

    # Mock '_file_transferred_to'
    mock_file_transferred_to = mocker.patch(
        "murfey.client.contexts.fib._file_transferred_to"
    )
    mock_file_transferred_to.return_value = (
        tmp_path
        / "fib"
        / "data"
        / "current_year"
        / visit_name
        / "autotem"
        / project_name
        / "ProjectData.dat"
    )
    # Set the expected output directory to be derived from metadata
    output_dir = (
        tmp_path
        / "fib"
        / "data"
        / "current_year"
        / visit_name
        / "processed"
        / project_name
        / "grid_2"
    )

    # Mock the functions used in 'post_transfer'
    mock_capture_post = mocker.patch("murfey.client.contexts.fib.capture_post")

    # Create the mock metadata file to parse
    mock_projectdata = create_fib_autotem_project_data(
        visit_dir=visit_dir,
        project_name=project_name,
        site_prefix="Lamella",
        has_project_name=has_project_name,
        has_sites=has_sites,
        has_site_name=has_site_name,
        has_recipes=has_recipes,
        has_recipe_name=has_recipe_name,
        has_activities=has_activities,
        has_activity_name=has_activity_name,
    )

    # Initialise the FIBContext
    basepath = visit_dir
    context = FIBContext(
        acquisition_software="autotem",
        basepath=basepath,
        machine_config={},
        token="",
    )
    if has_drift_correction_images:
        # Add drift correction images
        for i in range(num_lamellae):
            context._drift_correction_images[i + 1] = FIBImage(
                images=[tmp_path / "dummy.png"],
                output_file=None,
                is_submitted=False,
            )

    # Run 'post_transfer' and check for expected calls and outputs
    context._handle_autotem_metadata(mock_projectdata, environment=mock_environment)

    # Check the success case
    if all(
        (
            has_project_name,
            has_sites,
            has_site_name,
            has_recipes,
            has_recipe_name,
            has_activities,
            has_activity_name,
        )
    ):
        # 'capture_post' should be called once when registering the site
        # and again if registering a drift correction image
        assert mock_capture_post.call_count == num_lamellae * (
            2 if has_drift_correction_images else 1
        )
        # There should be one dictionary entry for each lamella now
        assert len(context._site_info) == num_lamellae
        for i in range(num_lamellae):
            lamella_number = i + 1
            mock_capture_post.assert_any_call(
                base_url=mock.ANY,
                router_name="workflow_fib.router",
                function_name="register_fib_milling_progress",
                token=mock.ANY,
                instrument_name=mock.ANY,
                data=mock.ANY,
                session_id=mock.ANY,
            )
            mock_logger.info.assert_any_call(
                f"Updating metadata for site {lamella_number}"
            )

            if has_drift_correction_images:
                mock_capture_post.assert_any_call(
                    base_url=mock.ANY,
                    router_name="workflow_fib.router",
                    function_name="make_gif",
                    token=mock.ANY,
                    instrument_name=mock.ANY,
                    data={
                        "lamella_number": lamella_number,
                        "images": [str(tmp_path / "dummy.png")],
                        "output_file": str(
                            output_dir
                            / "drift_correction"
                            / f"lamella_{lamella_number}.gif"
                        ),
                    },
                    session_id=mock.ANY,
                )

    # These test parameters are related, with one being False at a time
    # These fail cases will return an empty dict and not call "post_transfer"
    if not has_project_name:
        mock_logger.warning.assert_called_with("Metadata file has no project name")
        mock_capture_post.assert_not_called()
    elif not has_sites:
        mock_logger.warning.assert_called_with(
            f"No site information found in {str(mock_projectdata)}"
        )
        mock_capture_post.assert_not_called()
    elif not has_site_name:
        mock_logger.warning.assert_called_with("Current site doesn't have a name")
        mock_capture_post.assert_not_called()
    elif not has_recipes:
        for i in range(num_lamellae):
            site_name = "Lamella"
            if i > 0:
                site_name += f" ({i + 1})"
            mock_logger.warning.assert_any_call(
                f"No recipes found for site {site_name}"
            )
        mock_capture_post.assert_not_called()
    # These fail cases will produce LamellaSiteInfo dicts with default values
    # "capture_post" will still be called
    elif not has_recipe_name:
        mock_logger.warning.assert_any_call("Recipe doesn't have a name, skipping")
        assert mock_capture_post.call_count == num_lamellae
    elif not has_activities:
        for recipe_name in milling_steps.keys():
            mock_logger.warning.assert_any_call(
                f"Recipe {recipe_name} doesn't have any activities"
            )
        assert mock_capture_post.call_count == num_lamellae
    elif not has_activity_name:
        for recipe_name in milling_steps.keys():
            mock_logger.warning.assert_any_call(
                f"Activitiy in recipe {recipe_name} doesn't have a name, skipping"
            )
        assert mock_capture_post.call_count == num_lamellae


@pytest.mark.parametrize(
    "test_params",
    (
        # Early exits
        # No MurfeyInstanceEnvironment
        (False, True, True, True, True, True, True),
        # No source
        (True, False, True, True, True, True, True),
        # No destination
        (True, True, False, True, True, True, True),
        # No site info
        (True, True, True, False, True, True, True),
        # No project name
        (True, True, True, True, False, True, True),
        # No stage position
        (True, True, True, True, True, False, True),
        # No stage position values
        (True, True, True, True, True, True, False),
        # Successful case
        (True, True, True, True, True, True, True),
    ),
)
def test_fib_full_autotem_context_drift_correction_images(
    mocker: MockerFixture,
    test_params: tuple[bool, bool, bool, bool, bool, bool, bool],
    tmp_path: Path,
    visit_dir: Path,
    fib_autotem_dc_images: list[Path],
):
    # Unpack test params
    (
        use_env,
        find_source,
        find_dst,
        has_site_info,
        has_project_name,
        has_stage_position,
        has_stage_values,
    ) = test_params

    # Mock the environment
    mock_environment = None
    if use_env:
        mock_environment = MagicMock()
        mock_environment.visit = visit_name

    # Mock the logger to check if specific logs are triggered
    mock_logger = mocker.patch("murfey.client.contexts.fib.logger")

    # Create a list of destinations
    destination_dir = tmp_path / "fib" / "data" / "current_year" / visit_name
    destination_files = [
        destination_dir / file.relative_to(visit_dir) for file in fib_autotem_dc_images
    ]

    # Mock the functions used in 'post_transfer'
    mock_get_source = mocker.patch("murfey.client.contexts.fib._get_source")
    mock_get_source.return_value = tmp_path if find_source else None

    mock_file_transferred_to = mocker.patch(
        "murfey.client.contexts.fib._file_transferred_to"
    )
    if find_dst:
        mock_file_transferred_to.side_effect = destination_files
    else:
        mock_file_transferred_to.return_value = None

    mock_capture_post = mocker.patch("murfey.client.contexts.fib.capture_post")

    # Initialise the FIBContext
    basepath = tmp_path
    context = FIBContext(
        acquisition_software="autotem",
        basepath=basepath,
        machine_config={},
        token="",
    )

    # Create the Pydantic model for each site and add metadata
    for i in range(num_lamellae):
        lamella_num = i + 1
        metadata_dict = {
            "site_name": f"Lamella ({lamella_num})",
            "site_number": lamella_num,
        }
        if has_project_name:
            metadata_dict["project_name"] = project_name
        if has_stage_position:
            stage_dict: dict[str, dict] = {"preparation_site": {}}
            if has_stage_values:
                stage_dict["preparation_site"] = {"x": 0.003}
            metadata_dict["stage_info"] = stage_dict
        if has_site_info:
            context._site_info[lamella_num] = LamellaSiteInfo(**metadata_dict)

    # Parse images one-by-one and check that expected calls were made
    for file in fib_autotem_dc_images:
        context.post_transfer(file, environment=mock_environment)
    if not use_env:
        mock_logger.warning.assert_called_with("No environment passed in")
    elif not find_source:
        mock_logger.warning.assert_called_with(f"No source found for file {file}")
    elif not find_dst:
        mock_logger.warning.assert_called_with(
            f"Could not find destination file path for {file.name!r}"
        )
    elif not has_site_info:
        mock_logger.debug.assert_called_with(
            f"No metadata found for site {lamella_num} yet"
        )
    elif not has_project_name:
        mock_logger.warning.assert_any_call(
            f"No project name associated with site {lamella_num}"
        )
    elif not has_stage_position:
        mock_logger.warning.assert_any_call(
            f"No stage position information associated with site {lamella_num}"
        )
    elif not has_stage_values:
        mock_logger.warning.assert_any_call(
            f"Could not determine slot number of site {lamella_num}"
        )
    else:
        mock_get_source.assert_called_with(file, mock_environment)
        mock_file_transferred_to.assert_called_with(
            environment=mock_environment,
            source=basepath,
            file_path=file,
            rsync_basepath=Path(""),
        )
        assert len(context._drift_correction_images) == num_lamellae

        for i in range(num_lamellae):
            lamella_num = i + 1
            # The '_site_info' attribute should now be populated
            assert (
                context._site_info[lamella_num].stage_info.preparation_site.slot_number
                == 2
            )

            # The output file should point to 'grid_2' for a positive x stage position
            output_file = (
                tmp_path
                / "fib"
                / "data"
                / "current_year"
                / visit_name
                / "processed"
                / project_name
                / "grid_2"
                / "drift_correction"
                / f"lamella_{lamella_num}.gif"
            )
            assert (
                context._drift_correction_images[lamella_num].output_file == output_file
            )
        # 'capture_post' should be called for every image
        assert mock_capture_post.call_count == len(destination_files)


def test_fib_manual_autotem_context_projectdata(
    mocker: MockerFixture,
    visit_dir: Path,
):
    # Mock the ProjectData.dat file
    mock_projectdata = create_fib_autotem_project_data(
        visit_dir=visit_dir,
        project_name=f"AutoTEM_200101-1200_{project_name}",
        site_prefix="Site",
    )

    # Mock the Murfey environment
    mock_environment = MagicMock()
    mock_environment.visit = visit_name

    # Patch the '_parse_autotem_metadata' class function
    mock_parse = mocker.patch.object(FIBContext, "_parse_autotem_metadata")

    # Mock the functions used in 'post_transfer'
    mock_capture_post = mocker.patch("murfey.client.contexts.fib.capture_post")

    # Initialise the FIBContext
    basepath = visit_dir
    context = FIBContext(
        acquisition_software="autotem",
        basepath=basepath,
        machine_config={},
        token="",
    )

    # Pass file to FIBContext and check that it behaves as expected
    context.post_transfer(mock_projectdata, environment=mock_environment)
    mock_parse.assert_not_called()
    mock_capture_post.assert_not_called()


def test_fib_maps_context(
    mocker: MockerFixture,
    tmp_path: Path,
    visit_dir: Path,
    fib_maps_images: list[Path],
):
    # Mock the environment
    mock_environment = MagicMock()

    # Create a list of destinations
    destination_dir = tmp_path / "fib" / "data" / "current_year" / visit_name
    destination_files = [
        destination_dir / file.relative_to(visit_dir) for file in fib_maps_images
    ]

    # Mock the functions used in 'post_transfer'
    mock_get_source = mocker.patch(
        "murfey.client.contexts.fib._get_source", return_value=tmp_path
    )
    mock_file_transferred_to = mocker.patch(
        "murfey.client.contexts.fib._file_transferred_to", side_effect=destination_files
    )
    mock_capture_post = mocker.patch("murfey.client.contexts.fib.capture_post")

    # Initialise the FIBContext
    basepath = tmp_path
    context = FIBContext(
        acquisition_software="maps",
        basepath=basepath,
        machine_config={},
        token="",
    )

    # Parse images one-by-one
    for f, file in enumerate(fib_maps_images):
        context.post_transfer(file, environment=mock_environment)
        mock_get_source.assert_called_with(file, mock_environment)
        mock_file_transferred_to.assert_called_with(
            environment=mock_environment,
            source=basepath,
            file_path=file,
            rsync_basepath=Path(""),
        )
        mock_capture_post.assert_called_with(
            base_url=mock.ANY,
            router_name="workflow_fib.router",
            function_name="register_fib_atlas",
            token="",
            instrument_name=mock.ANY,
            data={"file": str(destination_files[f])},
            session_id=mock.ANY,
        )


def test_fib_meteor_context():
    pass
