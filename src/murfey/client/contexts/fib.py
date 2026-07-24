from __future__ import annotations

import logging
import threading
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Type, TypeVar, cast

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post
from murfey.util.fib import get_slot_number, number_from_name
from murfey.util.models import (
    LamellaSiteInfo,
    MillingStepInfo,
    MillingSteps,
    StagePositionInfo,
    StagePositionValues,
)

logger = logging.getLogger("murfey.client.contexts.fib")

lock = threading.Lock()


T = TypeVar("T")


def _parse_xml_text(
    node: ET.Element,
    path: str,
    func: Callable[[str], T] | Type,
) -> T | None:
    """
    Searches the XML Element using the provided path. If a matching node is found,
    and it has a text attribute, processes the text using the provided function.
    Otherwise, returns None.
    """
    if (match := node.find(path)) is None or (text := match.text) is None:
        return None
    try:
        return func(text)
    except (ValueError, TypeError):
        logger.error(f"Error parsing XML text {text} at path {path}", exc_info=True)
        return None


SI_UNITS_KEY = {
    # Length
    "mm": 1e-3,
    "um": 1e-6,
    "μm": 1e-6,
    "nm": 1e-9,
    # Current
    "mA": 1e-3,
    "uA": 1e-6,
    "μA": 1e-6,
    "nA": 1e-9,
    "pA": 1e-12,
    # Voltage
    "kV": 1e3,
    "mV": 1e-3,
    # Time
    "ms": 1e-3,
    "us": 1e-6,
    "μs": 1e-6,
    # Miscallenous
    "%": 0.01,
}


def _parse_measurement(text: str):
    """
    The measurements in the ProjectData.dat file are stored in a human-readable format
    as strings. This helper function converts them into their base SI unit and returns
    the value as a float.

    E.g. 5 um will be parsed as 0.000005
    """
    try:
        value, unit = (s.strip() for s in text.split(" ", 1))
        return float(value) * SI_UNITS_KEY.get(unit, 1)
    except ValueError:
        logger.warning(f"Could not parse {value} as a measurement")
        return None


def _parse_boolean(text: str):
    """
    Parses the XML element's text field and returns it as a Python boolean
    """
    if text.strip().lower() in ("true", "t", "1"):
        return True
    elif text.strip().lower() in ("false", "f", "0"):
        return False
    else:
        logger.warning(f"Could not parse {text} as a boolean")
        return None


MILLING_STEP_NAMES = {
    # Map unique activity name to class attribute
    # Preparation stage
    "Preparation - Eucentric Tilt": "eucentric_tilt",
    "Preparation - Artificial Features": "artificial_features",
    "Preparation - Milling Angle": "milling_angle",
    "Preparation - Image Acquisition": "image_acquisition",
    "Preparation - Lamella Placement": "lamella_placement",
    # Milling stage
    "Milling - Delay": "delay_1",
    "Milling - Reference Definition": "reference_definition",
    "Milling - Electron Reference Definition": "reference_definition_electron",
    "Milling - Stress Relief Cuts": "stress_relief_cuts",
    "Milling - Reference Redefinition 1": "reference_redefinition_1",
    "Milling - Rough Milling": "rough_milling",
    "Milling - Rough Milling - Electron Image": "rough_milling_electron",
    "Milling - Reference Redefinition 2": "reference_redefinition_2",
    "Milling - Medium Milling": "medium_milling",
    "Milling - Medium Milling - Electron Image": "medium_milling_electron",
    "Milling - Fine Milling": "fine_milling",
    "Milling - Fine Milling - Electron Image": "fine_milling_electron",
    "Milling - Finer Milling": "finer_milling",
    "Milling - Finer Milling - Electron Image": "finer_milling_electron",
    # Thinning stage
    "Thinning - Delay": "delay_2",
    "Thinning - Polishing 1": "polishing_1",
    "Thinning - Polishing 1 - Electron Image": "polishing_1_electron",
    "Thinning - Polishing 2": "polishing_2",
    "Thinning - Polishing 2 - Ion Image": "polishing_2_ion",
    "Thinning - Polishing 2 - Electron Image": "polishing_2_electron",
}


STAGE_POSITION_VALUES = {
    # Map class attribute to element name
    # Paths are relative to the "StagePosition" node
    "x": "X",
    "y": "Y",
    "z": "Z",
    "rotation": "R",
    "tilt_alpha": "AT",
}


STAGE_POSITION_NAMES = {
    # Map class attribute to element name
    # Paths are relative to the "Site" node
    "preparation_site": "PreparationSiteLocation/StagePosition/StagePosition",
    "chunk_site": "ChunkSiteLocation/StagePosition/StagePosition",
    "thinning_site": "ThinningSiteLocation/StagePosition/StagePosition",
    "chunk_coincidence_params": "Parameters/ChunkCoincidenceStagePosition/StagePosition",
    "thinning_params": "Parameters/ThinningStagePosition/StagePosition",
}


ACTIVITY_FIELD_MAP = (
    # Model field name | Path relative to "Activity" | Function to apply
    # These are relative to the "Activity" node
    # Common parameters
    ("is_enabled", "IsEnabled", _parse_boolean),
    ("status", "ActivityMetadata/ExecutionResult", str),
    ("execution_time", "ExecutionTime", _parse_measurement),
    # Milling/Imaging beam parameters
    ("site_location_type", "SiteLocationType", str),
    ("beam_type", "MillingPreset/BeamType", str),
    ("beam_type", "BeamPreset/BeamType", str),
    ("voltage", "MillingPreset/HighVoltage", _parse_measurement),
    ("voltage", "BeamPreset/HighVoltage", _parse_measurement),
    ("current", "MillingPreset/BeamCurrent", _parse_measurement),
    ("current", "BeamPreset/BeamCurrent", _parse_measurement),
    # Milling parameters
    ("depth_correction", "DepthCorrection", float),
    ("milling_angle", "MillingAngle", _parse_measurement),
    ("lamella_offset", "OffsetFromLamella", _parse_measurement),
    ("trench_height_front", "FrontTrenchHeight", _parse_measurement),
    ("trench_height_rear", "RearTrenchHeight", _parse_measurement),
    ("width_overlap_front_left", "LamellaFrontLeftWidthOverlap", _parse_measurement),
    ("width_overlap_front_right", "LamellaFrontRightWidthOverlap", _parse_measurement),
    ("width_overlap_rear_left", "LamellaRearLeftWidthOverlap", _parse_measurement),
    ("width_overlap_rear_right", "LamellaRearRightWidthOverlap", _parse_measurement),
)


def _get_project_name(file_path: Path):
    """
    Get the project name from the file path. This is used in manual AutoTEM
    workflows to identify the folder containing the images and site metadata
    to register.
    """
    try:
        autotem_idx = file_path.parts.index("autotem")
        project_dir = file_path.parents[-(autotem_idx + 2)]
        return project_dir.stem
    except Exception:
        logger.error(
            f"Error extracting project name from file path {file_path}:",
            exc_info=True,
        )
        return None


def _get_source(file_path: Path, environment: MurfeyInstanceEnvironment) -> Path | None:
    """
    Returns the Path of the file on the client PC.
    """
    for s in environment.sources:
        if file_path.is_relative_to(s):
            return s
    return None


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment,
    source: Path,
    file_path: Path,
    rsync_basepath: Path,
) -> Path | None:
    """
    Returns the Path of the transferred file on the DLS file system.
    """
    # Construct destination path
    base_destination = rsync_basepath / Path(environment.default_destinations[source])
    # Add visit number to the path if it's not present in default destination
    if environment.visit not in environment.default_destinations[source]:
        base_destination = base_destination / environment.visit
    destination = base_destination / file_path.relative_to(source)
    return destination


@dataclass
class FIBImage:
    images: list[Path] = field(default_factory=list)
    output_file: Path | None = None
    is_submitted: bool = False


class FIBContext(Context):
    def __init__(
        self,
        acquisition_software: str,
        basepath: Path,
        machine_config: dict,
        token: str,
    ):
        super().__init__("FIBContext", acquisition_software, token)
        self._basepath = basepath
        self._machine_config = machine_config
        self._project_data: dict[str, Path] = {}
        self._target_projects: list[str] = []
        self._site_info: dict[int, LamellaSiteInfo] = {}
        self._drift_correction_images: dict[int, FIBImage] = {}

    def post_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        super().post_transfer(transferred_file, environment=environment, **kwargs)
        if environment is None:
            logger.warning("No environment passed in")
            return None

        # -----------------------------------------------------------------------------
        # AutoTEM
        # -----------------------------------------------------------------------------
        if self._acquisition_software == "autotem":
            # Extract current project name from file path
            project_name = _get_project_name(transferred_file)
            if project_name is None:
                # Early exit if the check fails
                return None

            # Store incoming ProjectData.dat files in memory
            if (
                transferred_file.name == "ProjectData.dat"
                and self._project_data.get(project_name) is None
            ):
                self._project_data[project_name] = transferred_file

            # Identify if the current file's project is to be registered
            if project_name not in self._target_projects:
                if not any(
                    pattern in str(transferred_file)
                    for pattern in (
                        "/DCImages/",
                        "/LamellaEvaluationImages/",
                        "/Sites/Lamella",
                    )
                ):
                    # Early exit if the file is not from a relevant project
                    return None
                # Mark project folder for analysis
                self._target_projects.append(project_name)
                logger.info(
                    f"AutoTEM project {project_name!r} identified for registration"
                )

            # Analyse file and trigger processing only from target projects
            if project_name in self._target_projects:
                # Perform first-time metadata extraction using stored "ProjectData.dat" file
                if self._project_data.get(project_name) and not self._site_info:
                    project_data = self._project_data[project_name]
                    logger.info(
                        f"Performing initial metadata extraction from {project_data}"
                    )
                    self._handle_autotem_metadata(project_data, environment)
                # Extract metadata directly from "ProjectData.dat" on subsequent runs
                if transferred_file.name == "ProjectData.dat" and self._site_info:
                    logger.info(f"Found metadata file {transferred_file} for parsing")
                    self._handle_autotem_metadata(transferred_file, environment)
                    return None
                # Compile and register drift correction images
                elif (
                    "DCImages" in transferred_file.parts
                    and transferred_file.suffix == ".png"
                ):
                    self._make_drift_correction_gif(transferred_file, environment)
                    return None

        # -----------------------------------------------------------------------------
        # Maps
        # -----------------------------------------------------------------------------
        elif self._acquisition_software == "maps":
            if (
                # Electron snapshot images are grid atlases
                "Electron Snapshot" in transferred_file.name
                and transferred_file.suffix in (".tif", ".tiff")
            ):
                source = _get_source(transferred_file, environment)
                if source is None:
                    logger.warning(f"No source found for file {transferred_file}")
                    return None
                destination_file = _file_transferred_to(
                    environment=environment,
                    source=source,
                    file_path=transferred_file,
                    rsync_basepath=Path(self._machine_config.get("rsync_basepath", "")),
                )
                if destination_file is None:
                    logger.warning(
                        f"Could not find destination file path for {transferred_file.name!r}"
                    )
                    return None

                # Register image in database
                self._register_atlas(destination_file, environment)
                return None

        # -----------------------------------------------------------------------------
        # Meteor
        # -----------------------------------------------------------------------------
        elif self._acquisition_software == "meteor":
            pass

    def _parse_autotem_metadata(self, file: Path):
        """
        Helper function to parse the 'ProjectData.dat' file produced by the AutoTEM.
        This file contains metadata information on the milling sites set by the user,
        along with the configured milling steps and their completion status.
        """

        all_site_info: dict[int, LamellaSiteInfo] = {}
        try:
            root = ET.parse(file).getroot()
        except Exception:
            logger.warning(f"Error parsing file {str(file)}", exc_info=True)
            return all_site_info

        # Get the project name
        if (project_name := _parse_xml_text(root, ".//Project/Name", str)) is None:
            logger.warning("Metadata file has no project name")
            return all_site_info

        # Find all the Site nodes
        if not (sites := root.findall(".//Sites/Site")):
            logger.warning(f"No site information found in {str(file)}")
            return all_site_info

        # Iterate through Site nodes
        for site in sites:
            # Extract site name and number
            if (site_name := _parse_xml_text(site, "Name", str)) is None:
                logger.warning("Current site doesn't have a name")
                continue
            site_num = number_from_name(site_name)
            site_info = LamellaSiteInfo(
                project_name=project_name,
                site_name=site_name,
                site_number=site_num,
                steps=MillingSteps(),
            )

            # Extract stage position information for all known stages in current site
            site_info.stage_info = StagePositionInfo()
            for stage_name, stage_path in STAGE_POSITION_NAMES.items():
                if (stage := site.find(stage_path)) is not None:
                    stage_values = StagePositionValues()
                    for value_name, value_path in STAGE_POSITION_VALUES.items():
                        if (
                            value := _parse_xml_text(
                                stage, value_path, _parse_measurement
                            )
                        ) is not None:
                            stage_values.__setattr__(value_name, value)
                    site_info.stage_info.__setattr__(stage_name, stage_values)

            # Find all Recipe nodes for the Site
            if not (recipes := site.findall("Workflow/Recipe")):
                # Early skip if no recipes are found
                logger.warning(f"No recipes found for site {site_name}")
                continue

            # Create dataclasses for each site
            for recipe in recipes:
                if (recipe_name := _parse_xml_text(recipe, "Name", str)) is None:
                    # Early skip if the Recipe has no Name
                    logger.warning("Recipe doesn't have a name, skipping")
                    continue

                # Find all the nodes under Activities
                if (activities := recipe.find("Activities")) is None:
                    # Early skip if none exist
                    logger.warning(f"Recipe {recipe_name} doesn't have any activities")
                    continue

                # Iterate through the activities
                for activity in activities:
                    if (
                        activity_name := _parse_xml_text(activity, "Name", str)
                    ) is None:
                        # Early skip if activity has no name
                        logger.warning(
                            f"Activitiy in recipe {recipe_name} doesn't have a name, skipping"
                        )
                        continue

                    # Create a unique name based on recipe and activity names
                    unique_name = f"{recipe_name} - {activity_name}"
                    step_info = MillingStepInfo(
                        step_name=activity_name, recipe_name=recipe_name
                    )

                    # Iteratively update fields in the MillingSteps model it's not None
                    for field_name, path, func in ACTIVITY_FIELD_MAP:
                        if (value := _parse_xml_text(activity, path, func)) is not None:
                            step_info.__setattr__(field_name, value)

                    # Add info for current step to the site info model
                    site_info.steps.__setattr__(
                        MILLING_STEP_NAMES[unique_name], step_info
                    )

            # Add info for current site to the dict
            all_site_info[site_num] = site_info

        logger.info(f"Successfully extracted AutoTEM metadata from file {file}")
        return all_site_info

    def _determine_output_dir(
        self,
        lamella_number: int,
        destination_file: Path,
        environment: MurfeyInstanceEnvironment,
    ):
        """
        Helper function to determine the output directory for the current lamella site
        on the server side.
        """
        # Early exits if data for creating output path is absent
        # No site info
        if (site_info := self._site_info.get(lamella_number)) is None:
            logger.debug(f"No metadata found for site {lamella_number} yet")
            return None
        # No project name
        if (project_name := site_info.project_name) is None:
            logger.warning(f"No project name associated with site {lamella_number}")
            return None
        # No stage position information
        if all(
            getattr(site_info.stage_info, stage_name, None) is None
            for stage_name in STAGE_POSITION_NAMES.keys()
        ):
            logger.warning(
                f"No stage position information associated with site {lamella_number}"
            )
            return None
        # Determine the slot number
        slot_number: int | None = None
        for stage_name in reversed(STAGE_POSITION_NAMES.keys()):
            stage_values: StagePositionValues | None = getattr(
                site_info.stage_info, stage_name, None
            )
            if stage_values is None:
                continue
            else:
                rotation_offset = cast(
                    float,
                    self._machine_config.get("calibrations", {}).get(
                        "rotation_offset", 0
                    ),
                )
                slot_number = get_slot_number(
                    x=stage_values.x,
                    y=stage_values.y,
                    rotation=stage_values.rotation,
                    rotation_offset=rotation_offset,
                )
                break
        # Early exit if no slot number
        if slot_number is None:
            logger.warning(f"Could not determine slot number of site {lamella_number}")
            return None
        # Determine the path to save the GIF to
        try:
            visit_index = destination_file.parts.index(environment.visit)
            visit_dir = list(reversed(destination_file.parents))[visit_index]
            return visit_dir / "processed" / project_name / f"grid_{slot_number}"
        except Exception:
            logger.error(
                f"Could not construct output directory path for site {lamella_number}"
            )
            return None

    def _handle_autotem_metadata(
        self, file: Path, environment: MurfeyInstanceEnvironment
    ):
        """
        Helper function to extract the AutoTEM metadata, update the stored FIB lamella
        site info, and trigger relevant processing.
        """

        # Extract all site info
        all_site_info_new = self._parse_autotem_metadata(file)

        # Parse the metadata file
        for site_num, site_info_new in all_site_info_new.items():
            # Post the data to the backend if it's been changed
            if (
                data := site_info_new.model_dump(exclude_none=True)
            ) != self._site_info.get(site_num, LamellaSiteInfo()).model_dump(
                exclude_none=True
            ):
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="workflow_fib.router",
                    function_name="register_fib_milling_progress",
                    token=self._token,
                    instrument_name=environment.instrument_name,
                    data=data,
                    # Endpoint kwargs
                    session_id=environment.murfey_session,
                )

                # Update existing dict
                self._site_info[site_num] = site_info_new
                logger.info(f"Updating metadata for site {site_num}")

            # Post drift correction GIF request if it hasn't already been done
            fib_image = self._drift_correction_images.get(site_num, None)
            if fib_image is not None and not fib_image.is_submitted:
                self._make_drift_correction_gif(
                    fib_image.images[-1],
                    environment,
                    is_destination_file=True,
                )
        return None

    def _make_drift_correction_gif(
        self,
        file: Path,
        environment: MurfeyInstanceEnvironment,
        is_destination_file: bool = False,
    ):
        """
        Helper function to create GIFs using the drift correction images seen by the
        FIBContext class. The function uses the metadata extracted from the
        """
        parts = file.parts
        try:
            lamella_name = parts[parts.index("Sites") + 1]
            lamella_number = number_from_name(lamella_name)
        except Exception:
            logger.warning(
                f"Could not extract metadata from file {file}", exc_info=True
            )
            return None

        # If the file provided is client-side, construct the destination file path
        if not is_destination_file:
            source = _get_source(file, environment)
            if source is None:
                logger.warning(f"No source found for file {file}")
                return
            destination_file = _file_transferred_to(
                environment=environment,
                source=source,
                file_path=file,
                rsync_basepath=Path(self._machine_config.get("rsync_basepath", "")),
            )
            if destination_file is None:
                logger.warning(
                    f"Could not find destination file path for {file.name!r}"
                )
                return
        else:
            destination_file = file

        # Create FIBImage instance for this lamella site, or update existing one
        if not self._drift_correction_images.get(lamella_number):
            with lock:
                self._drift_correction_images[lamella_number] = FIBImage(
                    images=[destination_file]
                )
        # Only update list if the file is not already in it
        elif (
            destination_file not in self._drift_correction_images[lamella_number].images
        ):
            with lock:
                self._drift_correction_images[lamella_number].images.append(
                    destination_file
                )
                self._drift_correction_images[lamella_number].is_submitted = False

        # If output GIF file path has not already been determined, construct it
        output_file = self._drift_correction_images[lamella_number].output_file
        if output_file is None:
            if (
                output_dir := self._determine_output_dir(
                    lamella_number,
                    destination_file,
                    environment,
                )
            ) is None:
                logger.warning(
                    f"Could not determine output directory for lamella {lamella_number}"
                )
                return None
            output_file = (
                output_dir / "drift_correction" / f"lamella_{lamella_number}.gif"
            )
            with lock:
                self._drift_correction_images[lamella_number].output_file = output_file

        # Submit job to backend to construct a GIF
        if self._make_gif(
            environment=environment,
            lamella_number=lamella_number,
            images=sorted(self._drift_correction_images[lamella_number].images),
            output_file=output_file,
        ):
            # Mark this dataset as having been submitted
            with lock:
                self._drift_correction_images[lamella_number].is_submitted = True
            logger.info(
                f"Submitted request to create drift correction GIF for site {lamella_number}"
            )
        return None

    def _make_gif(
        self,
        environment: MurfeyInstanceEnvironment,
        lamella_number: int,
        images: list[Path],
        output_file: Path,
    ):
        """
        Submits a POST request to the backend server to create a GIF using the
        JSON payload provided. The payload will contain
        """
        try:
            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="workflow_fib.router",
                function_name="make_gif",
                token=self._token,
                instrument_name=environment.instrument_name,
                data={
                    "lamella_number": lamella_number,
                    "images": [str(file) for file in images],
                    "output_file": str(output_file),
                },
                # Endpoint kwargs
                session_id=environment.murfey_session,
            )
            return True
        except Exception:
            logger.error(f"Could not submit GIF for site {lamella_number}")
            return False

    def _register_atlas(self, file: Path, environment: MurfeyInstanceEnvironment):
        """
        Constructs the URL and dictionary to be posted to the server, which then triggers
        the processing of the electron snapshot image.
        """

        try:
            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="workflow_fib.router",
                function_name="register_fib_atlas",
                token=self._token,
                instrument_name=environment.instrument_name,
                data={"file": str(file)},
                # Endpoint kwargs
                session_id=environment.murfey_session,
            )
            logger.info(f"Registering atlas image {file.name!r}")
            return True
        except Exception as e:
            logger.error(f"Error encountered registering atlas image {file.name}:\n{e}")
            return False
