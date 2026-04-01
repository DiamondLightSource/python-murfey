"""
Contains classes that are used to store information on the metadata and status of jobs
of the sessions that Murfey is overseeing, along with the relationships between them.
"""

from datetime import datetime
from typing import List, Optional

import sqlalchemy
from sqlmodel import Enum, Field, Relationship, SQLModel, create_engine

"""
GENERAL
"""


class MurfeyUser(SQLModel, table=True):  # type: ignore
    username: str = Field(primary_key=True)
    hashed_password: str


class MagnificationLookup(SQLModel, table=True):  # type: ignore
    magnification: int = Field(primary_key=True)
    pixel_size: float = Field(primary_key=True)


class ClientEnvironment(SQLModel, table=True):  # type: ignore
    client_id: Optional[int] = Field(primary_key=True, unique=True)
    visit: str = Field(default="")
    session_id: Optional[int] = Field(foreign_key="session.id")
    connected: bool


class RsyncInstance(SQLModel, table=True):  # type: ignore
    source: str = Field(primary_key=True)
    destination: str = Field(primary_key=True, default="")
    session_id: int = Field(foreign_key="session.id", primary_key=True)
    tag: str = Field(default="")
    files_transferred: int = Field(default=0)
    files_counted: int = Field(default=0)
    transferring: bool = Field(default=False)
    session: Optional["Session"] = Relationship(back_populates="rsync_instances")


class Session(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True)
    name: str
    visit: str = Field(default="")
    started: bool = Field(default=False)
    current_gain_ref: str = Field(default="")
    instrument_name: str = Field(default="")
    process: bool = Field(default=True)
    visit_end_time: Optional[datetime] = Field(default=None)

    # Image sites associated with this session
    imaging_sites: List["ImagingSite"] = Relationship(
        back_populates="session",
        sa_relationship_kwargs={"cascade": "delete"},
    )

    # TEM Workflow

    tilt_series: List["TiltSeries"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    data_collection_groups: List["DataCollectionGroup"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    preprocess_stashes: List["PreprocessStash"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    grid_squares: List["GridSquare"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    foil_holes: List["FoilHole"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    search_maps: List["SearchMap"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    rsync_instances: List[RsyncInstance] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    session_processing_parameters: List["SessionProcessingParameters"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )


class ImagingSite(SQLModel, table=True):  # type: ignore
    """
    Table for recording unique imaging sites in the session. These can then be linked
    to DataCollectionGroup or GridSquare entries as needed.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    site_name: str = Field(index=True)

    # File paths to images and thumbnails; can be a glob search string
    image_path: Optional[str] = Field(default=None)
    thumbnail_path: Optional[str] = Field(default=None)

    # Link to Session table
    session: Optional["Session"] = Relationship(
        back_populates="imaging_sites"
    )  # Many to one
    session_id: Optional[int] = Field(
        foreign_key="session.id", default=None, unique=False
    )

    # Type of data (atlas/overview or grid square)
    data_type: Optional[str] = Field(default=None)  # "atlas" or "grid_square"

    # Link to data collection group
    data_collection_group: Optional["DataCollectionGroup"] = Relationship(
        back_populates="imaging_sites"
    )
    dcg_id: Optional[int] = Field(
        foreign_key="datacollectiongroup.dataCollectionGroupId", default=None
    )
    dcg_name: Optional[str] = Field(default=None)

    # Link to grid squares
    grid_square: Optional["GridSquare"] = Relationship(back_populates="imaging_sites")
    grid_square_id: Optional[int] = Field(foreign_key="gridsquare.id", default=None)

    # Shape and resolution information
    image_pixels_x: Optional[int] = Field(default=None)
    image_pixels_y: Optional[int] = Field(default=None)
    image_pixel_size: Optional[float] = Field(default=None)
    thumbnail_pixels_x: Optional[int] = Field(default=None)
    thumbnail_pixels_y: Optional[int] = Field(default=None)
    thumbnail_pixel_size: Optional[float] = Field(default=None)
    units: Optional[str] = Field(default=None)

    # Extent of the imaged area in real space
    x0: Optional[float] = Field(default=None)
    x1: Optional[float] = Field(default=None)
    y0: Optional[float] = Field(default=None)
    y1: Optional[float] = Field(default=None)

    # Colour channel-related fields
    number_of_members: Optional[int] = Field(default=None)
    has_grey: Optional[bool] = Field(default=None)
    has_red: Optional[bool] = Field(default=None)
    has_green: Optional[bool] = Field(default=None)
    has_blue: Optional[bool] = Field(default=None)
    has_cyan: Optional[bool] = Field(default=None)
    has_magenta: Optional[bool] = Field(default=None)
    has_yellow: Optional[bool] = Field(default=None)
    collection_mode: Optional[str] = Field(default=None)
    composite_created: bool = False  # Has a composite image been created?


"""
TEM SESSION AND PROCESSING WORKFLOW
"""


class SessionProcessingParameters(SQLModel, table=True):  # type: ignore
    session_id: int = Field(foreign_key="session.id", primary_key=True)
    gain_ref: str
    dose_per_frame: float
    eer_fractionation: int = 20
    eer_fractionation_file: str = ""
    symmetry: str = "C1"
    run_class3d: bool = True
    session: Optional[Session] = Relationship(
        back_populates="session_processing_parameters"
    )


class TiltSeries(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True)
    ispyb_id: Optional[int] = None
    tag: str
    rsync_source: str
    session_id: int = Field(foreign_key="session.id")
    search_map_id: Optional[int] = Field(
        foreign_key="searchmap.id",
        default=None,
    )
    tilt_series_length: int = -1
    processing_requested: bool = False
    x_location: Optional[float] = None
    y_location: Optional[float] = None
    session: Optional[Session] = Relationship(back_populates="tilt_series")
    tilts: List["Tilt"] = Relationship(
        back_populates="tilt_series", sa_relationship_kwargs={"cascade": "delete"}
    )
    search_map: Optional["SearchMap"] = Relationship(back_populates="tilt_series")


class Tilt(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True)
    movie_path: str
    tilt_series_id: int = Field(foreign_key="tiltseries.id")
    motion_corrected: bool = False
    tilt_series: Optional[TiltSeries] = Relationship(back_populates="tilts")


class DataCollectionGroup(SQLModel, table=True):  # type: ignore
    id: int = Field(
        primary_key=True,
        unique=True,
        alias="dataCollectionGroupId",
        sa_column_kwargs={"name": "dataCollectionGroupId"},
    )
    session_id: int = Field(foreign_key="session.id", primary_key=True)
    tag: str = Field(primary_key=True)
    atlas_id: Optional[int] = None
    atlas_pixel_size: Optional[float] = None
    atlas: str = ""
    sample: Optional[int] = None
    smartem_grid_uuid: Optional[str] = None
    session: Optional["Session"] = Relationship(back_populates="data_collection_groups")
    data_collections: List["DataCollection"] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    imaging_sites: List["ImagingSite"] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    notification_parameters: List["NotificationParameter"] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    tomography_processing_parameters: List["TomographyProcessingParameters"] = (
        Relationship(
            back_populates="data_collection_group",
            sa_relationship_kwargs={"cascade": "delete"},
        )
    )
    grid_squares: Optional[List["GridSquare"]] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    search_maps: Optional[List["SearchMap"]] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )


class NotificationParameter(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    dcg_id: int = Field(foreign_key="datacollectiongroup.dataCollectionGroupId")
    name: str
    min_value: float
    max_value: float
    num_instances_since_triggered: int = 0
    notification_active: bool = False
    data_collection_group: Optional[DataCollectionGroup] = Relationship(
        back_populates="notification_parameters"
    )
    notification_values: List["NotificationValue"] = Relationship(
        back_populates="notification_parameter",
        sa_relationship_kwargs={"cascade": "delete"},
    )


class NotificationValue(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    notification_parameter_id: int = Field(foreign_key="notificationparameter.id")
    index: int
    within_bounds: bool
    notification_parameter: Optional[NotificationParameter] = Relationship(
        back_populates="notification_values"
    )


class DataCollection(SQLModel, table=True):  # type: ignore
    id: int = Field(
        primary_key=True,
        unique=True,
        alias="dataCollectionId",
        sa_column_kwargs={"name": "dataCollectionId"},
    )
    tag: str = Field(primary_key=True)
    dcg_id: int = Field(
        foreign_key="datacollectiongroup.dataCollectionGroupId",
        alias="dataCollectionGroupId",
        sa_column_kwargs={"name": "dataCollectionGroupId"},
    )
    data_collection_group: Optional[DataCollectionGroup] = Relationship(
        back_populates="data_collections"
    )
    processing_jobs: List["ProcessingJob"] = Relationship(
        back_populates="data_collection", sa_relationship_kwargs={"cascade": "delete"}
    )
    movies: List["Movie"] = Relationship(
        back_populates="data_collection", sa_relationship_kwargs={"cascade": "delete"}
    )
    motion_correction: Optional[List["MotionCorrection"]] = Relationship(
        back_populates="data_collection"
    )
    tomogram: Optional[List["Tomogram"]] = Relationship(
        back_populates="data_collection"
    )


class ProcessingJob(SQLModel, table=True):  # type: ignore
    id: int = Field(
        primary_key=True,
        unique=True,
        alias="processingJobId",
        sa_column_kwargs={"name": "processingJobId"},
    )
    recipe: str = Field(primary_key=True)
    dc_id: int = Field(
        foreign_key="datacollection.dataCollectionId",
        alias="dataCollectionId",
        sa_column_kwargs={"name": "dataCollectionId"},
    )
    data_collection: Optional[DataCollection] = Relationship(
        back_populates="processing_jobs"
    )

    auto_proc_programs: List["AutoProcProgram"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    selection_stash: List["SelectionStash"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    particle_sizes: List["ParticleSizes"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    spa_parameters: List["SPARelionParameters"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    classification_feedback_parameters: List["ClassificationFeedbackParameters"] = (
        Relationship(
            back_populates="processing_job",
            sa_relationship_kwargs={"cascade": "delete"},
        )
    )
    ctf_parameters: List["CtfParameters"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    tomogram_picks: List["TomogramPicks"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    class2d_parameters: List["Class2DParameters"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    class3d_parameters: List["Class3DParameters"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    refine_parameters: List["RefineParameters"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    class2ds: List["Class2D"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    class3ds: List["Class3D"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    refine3ds: List["Refine3D"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )


class PreprocessStash(SQLModel, table=True):  # type: ignore
    file_path: str = Field(primary_key=True)
    tag: str = Field(primary_key=True)
    session_id: int = Field(primary_key=True, foreign_key="session.id")
    foil_hole_id: Optional[int] = Field(foreign_key="foilhole.id", default=None)
    image_number: int
    mrc_out: str
    eer_fractionation_file: Optional[str]
    group_tag: Optional[str]
    session: Optional[Session] = Relationship(back_populates="preprocess_stashes")
    foil_hole: Optional["FoilHole"] = Relationship(back_populates="preprocess_stashes")


class SelectionStash(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    class_selection_score: float
    pj_id: int = Field(foreign_key="processingjob.processingJobId")
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="selection_stash"
    )


class TomographyProcessingParameters(SQLModel, table=True):  # type: ignore
    dcg_id: int = Field(
        primary_key=True, foreign_key="datacollectiongroup.dataCollectionGroupId"
    )
    pixel_size: float
    dose_per_frame: float
    frame_count: int
    tilt_axis: float
    voltage: int
    particle_diameter: Optional[float] = None
    eer_fractionation_file: Optional[str] = None
    motion_corr_binning: int = 1
    gain_ref: Optional[str] = None
    data_collection_group: Optional[DataCollectionGroup] = Relationship(
        back_populates="tomography_processing_parameters"
    )


class AutoProcProgram(SQLModel, table=True):  # type: ignore
    id: int = Field(
        primary_key=True,
        unique=True,
        alias="autoProcProgramId",
        sa_column_kwargs={"name": "autoProcProgramId"},
    )
    pj_id: int = Field(
        foreign_key="processingjob.processingJobId",
        alias="processingJobId",
        sa_column_kwargs={"name": "processingJobId"},
    )
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="auto_proc_programs"
    )
    murfey_ids: List["MurfeyLedger"] = Relationship(
        back_populates="auto_proc_program", sa_relationship_kwargs={"cascade": "delete"}
    )
    motion_correction: Optional[List["MotionCorrection"]] = Relationship(
        back_populates="auto_proc_program"
    )
    tomogram: Optional[List["Tomogram"]] = Relationship(
        back_populates="auto_proc_program"
    )
    ctf: Optional[List["CTF"]] = Relationship(back_populates="auto_proc_program")
    particle_picker: Optional[List["ParticlePicker"]] = Relationship(
        back_populates="auto_proc_program"
    )
    relative_ice_thickness: Optional[List["RelativeIceThickness"]] = Relationship(
        back_populates="auto_proc_program"
    )
    particle_classification_group: Optional[List["ParticleClassificationGroup"]] = (
        Relationship(back_populates="auto_proc_program")
    )


class MurfeyLedger(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(primary_key=True, default=None)
    app_id: int = Field(foreign_key="autoprocprogram.autoProcProgramId")
    auto_proc_program: Optional[AutoProcProgram] = Relationship(
        back_populates="murfey_ids"
    )
    class2ds: Optional["Class2D"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
    )
    class3ds: Optional["Class3D"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
    )
    refine3ds: Optional["Refine3D"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
    )
    class2d_parameters: Optional["Class2DParameters"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
    )
    class3d_parameters: Optional["Class3DParameters"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
    )
    refine_parameters: Optional["RefineParameters"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
    )
    classification_feedback_parameters: Optional["ClassificationFeedbackParameters"] = (
        Relationship(
            back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
        )
    )
    movies: Optional["Movie"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
    )


class GridSquare(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(primary_key=True, default=None)
    session_id: int = Field(foreign_key="session.id")
    name: int
    tag: str
    x_location: Optional[float]
    y_location: Optional[float]
    x_stage_position: Optional[float]
    y_stage_position: Optional[float]
    readout_area_x: Optional[int]
    readout_area_y: Optional[int]
    thumbnail_size_x: Optional[int]
    thumbnail_size_y: Optional[int]
    pixel_size: Optional[float] = None
    image: str = ""
    session: Optional[Session] = Relationship(back_populates="grid_squares")
    imaging_sites: List["ImagingSite"] = Relationship(
        back_populates="grid_square", sa_relationship_kwargs={"cascade": "delete"}
    )
    foil_holes: List["FoilHole"] = Relationship(
        back_populates="grid_square", sa_relationship_kwargs={"cascade": "delete"}
    )
    atlas_id: Optional[int] = Field(
        foreign_key="datacollectiongroup.dataCollectionGroupId"
    )
    scaled_pixel_size: Optional[float] = None
    pixel_location_x: Optional[int] = None
    pixel_location_y: Optional[int] = None
    height: Optional[int] = None
    width: Optional[int] = None
    angle: Optional[float] = None
    quality_indicator: Optional[float] = None
    smartem_uuid: Optional[str] = None
    data_collection_group: Optional["DataCollectionGroup"] = Relationship(
        back_populates="grid_squares"
    )


class FoilHole(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(primary_key=True, default=None)
    grid_square_id: int = Field(foreign_key="gridsquare.id")
    session_id: int = Field(foreign_key="session.id")
    name: int
    x_location: Optional[float]
    y_location: Optional[float]
    x_stage_position: Optional[float]
    y_stage_position: Optional[float]
    readout_area_x: Optional[int]
    readout_area_y: Optional[int]
    thumbnail_size_x: Optional[int]
    thumbnail_size_y: Optional[int]
    pixel_size: Optional[float] = None
    image: str = ""
    grid_square: Optional[GridSquare] = Relationship(back_populates="foil_holes")
    session: Optional[Session] = Relationship(back_populates="foil_holes")
    movies: List["Movie"] = Relationship(
        back_populates="foil_hole", sa_relationship_kwargs={"cascade": "delete"}
    )
    preprocess_stashes: List[PreprocessStash] = Relationship(
        back_populates="foil_hole", sa_relationship_kwargs={"cascade": "delete"}
    )
    scaled_pixel_size: Optional[float] = None
    pixel_location_x: Optional[int] = None
    pixel_location_y: Optional[int] = None
    diameter: Optional[int] = None
    quality_indicator: Optional[float] = None
    smartem_uuid: Optional[str] = None


class SearchMap(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(primary_key=True, default=None)
    session_id: int = Field(foreign_key="session.id")
    name: str
    tag: str
    x_location: Optional[float] = None
    y_location: Optional[float] = None
    x_stage_position: Optional[float] = None
    y_stage_position: Optional[float] = None
    pixel_size: Optional[float] = None
    image: str = ""
    binning: Optional[float] = None
    reference_matrix_m11: Optional[float] = None
    reference_matrix_m12: Optional[float] = None
    reference_matrix_m21: Optional[float] = None
    reference_matrix_m22: Optional[float] = None
    stage_correction_m11: Optional[float] = None
    stage_correction_m12: Optional[float] = None
    stage_correction_m21: Optional[float] = None
    stage_correction_m22: Optional[float] = None
    image_shift_correction_m11: Optional[float] = None
    image_shift_correction_m12: Optional[float] = None
    image_shift_correction_m21: Optional[float] = None
    image_shift_correction_m22: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    session: Optional[Session] = Relationship(back_populates="search_maps")
    tilt_series: List["TiltSeries"] = Relationship(
        back_populates="search_map", sa_relationship_kwargs={"cascade": "delete"}
    )
    atlas_id: Optional[int] = Field(
        foreign_key="datacollectiongroup.dataCollectionGroupId"
    )
    scaled_pixel_size: Optional[float] = None
    pixel_location_x: Optional[int] = None
    pixel_location_y: Optional[int] = None
    scaled_height: Optional[int] = None
    scaled_width: Optional[int] = None
    angle: Optional[float] = None
    quality_indicator: Optional[float] = None
    data_collection_group: Optional["DataCollectionGroup"] = Relationship(
        back_populates="search_maps"
    )
    tomogram: Optional[List["Tomogram"]] = Relationship(back_populates="search_map")


class Movie(SQLModel, table=True):  # type: ignore
    murfey_id: int = Field(
        primary_key=True,
        foreign_key="murfeyledger.id",
        alias="movieId",
        sa_column_kwargs={"name": "movieId"},
    )
    data_collection_id: Optional[int] = Field(
        foreign_key="datacollection.dataCollectionId",
        alias="dataCollectionId",
        sa_column_kwargs={"name": "dataCollectionId"},
    )
    foil_hole_id: int = Field(foreign_key="foilhole.id", nullable=True, default=None)
    image_number: int = Field(
        alias="movieNumber", sa_column_kwargs={"name": "movieNumber"}
    )
    path: str = Field(alias="imageFullPath", sa_column_kwargs={"name": "imageFullPath"})
    creation_time: datetime = Field(
        alias="createdTimeStamp",
        sa_column_kwargs={"name": "createdTimeStamp"},
        default_factory=datetime.now,
    )
    tag: str
    preprocessed: bool = False
    positionX: Optional[float] = None
    positionY: Optional[float] = None
    nominalDefocus: Optional[float] = None
    angle: Optional[float] = None
    fluence: Optional[float] = None
    numberOfFrames: Optional[int] = None
    templateLabel: Optional[str] = None
    smartem_uuid: Optional[str] = None
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="movies")
    data_collection: Optional["DataCollection"] = Relationship(back_populates="movies")
    foil_hole: Optional[FoilHole] = Relationship(back_populates="movies")
    motion_correction: Optional[List["MotionCorrection"]] = Relationship(
        back_populates="movie"
    )
    tilt_image_alignment: Optional[List["TiltImageAlignment"]] = Relationship(
        back_populates="movie"
    )


class CtfParameters(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    pj_id: int = Field(foreign_key="processingjob.processingJobId")
    micrographs_file: str
    coord_list_file: str
    extract_file: str
    ctf_image: str
    ctf_max_resolution: float
    ctf_figure_of_merit: float
    defocus_u: float
    defocus_v: float
    defocus_angle: float
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="ctf_parameters"
    )


class TomogramPicks(SQLModel, table=True):  # type: ignore
    tomogram: str = Field(primary_key=True)
    pj_id: int = Field(foreign_key="processingjob.processingJobId")
    cbox_3d: str
    particle_count: int
    tomogram_pixel_size: float
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="tomogram_picks"
    )


class ParticleSizes(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    pj_id: int = Field(foreign_key="processingjob.processingJobId")
    particle_size: float
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="particle_sizes"
    )


class SPARelionParameters(SQLModel, table=True):  # type: ignore
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    angpix: float
    dose_per_frame: float
    gain_ref: Optional[str]
    voltage: int
    motion_corr_binning: int
    eer_fractionation_file: str = ""
    symmetry: str
    particle_diameter: Optional[float]
    downscale: bool = True
    do_icebreaker_jobs: bool = True
    boxsize: Optional[int] = 256
    small_boxsize: Optional[int] = 64
    mask_diameter: Optional[float] = 190
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="spa_parameters"
    )


class ClassificationFeedbackParameters(SQLModel, table=True):  # type: ignore
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    estimate_particle_diameter: bool = True
    hold_class2d: bool = False
    rerun_class2d: bool = False
    hold_class3d: bool = False
    hold_refine: bool = False
    class_selection_score: float
    star_combination_job: int
    initial_model: str
    next_job: int
    picker_murfey_id: Optional[int] = Field(default=None, foreign_key="murfeyledger.id")
    picker_ispyb_id: Optional[int] = None
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="classification_feedback_parameters"
    )
    murfey_ledger: Optional[MurfeyLedger] = Relationship(
        back_populates="classification_feedback_parameters"
    )


class Class2DParameters(SQLModel, table=True):  # type: ignore
    particles_file: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    class2d_dir: str
    batch_size: int
    complete: bool = True
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="class2d_parameters"
    )
    murfey_ledger: Optional[MurfeyLedger] = Relationship(
        back_populates="class2d_parameters"
    )


class Class2D(SQLModel, table=True):  # type: ignore
    class_number: int = Field(primary_key=True)
    particles_file: str = Field(
        primary_key=True,
    )
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    processing_job: Optional[ProcessingJob] = Relationship(back_populates="class2ds")
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="class2ds")


class Class3DParameters(SQLModel, table=True):  # type: ignore
    particles_file: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    class3d_dir: str
    batch_size: int
    run: bool = False
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="class3d_parameters"
    )
    murfey_ledger: Optional[MurfeyLedger] = Relationship(
        back_populates="class3d_parameters"
    )
    # class3ds: List["Class3D"] = Relationship(
    #    back_populates="class3d_parameters",
    #    sa_relationship_kwargs={"cascade": "delete"},
    # )


class Class3D(SQLModel, table=True):  # type: ignore
    class_number: int = Field(primary_key=True)
    particles_file: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    # class3d_parameters: Optional[Class3DParameters] = Relationship(
    #    back_populates="class3ds"
    # )
    processing_job: Optional[ProcessingJob] = Relationship(back_populates="class3ds")
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="class3ds")


class RefineParameters(SQLModel, table=True):  # type: ignore
    tag: str = Field(primary_key=True)
    refine_dir: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    class3d_dir: str
    class_number: int
    run: bool = False
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="refine_parameters"
    )
    murfey_ledger: Optional[MurfeyLedger] = Relationship(
        back_populates="refine_parameters"
    )


class Refine3D(SQLModel, table=True):  # type: ignore
    tag: str = Field(primary_key=True)
    refine_dir: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    processing_job: Optional[ProcessingJob] = Relationship(back_populates="refine3ds")
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="refine3ds")


class BFactorParameters(SQLModel, table=True):  # type: ignore
    project_dir: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    batch_size: int
    refined_grp_uuid: int
    refined_class_uuid: int
    class_reference: str
    class_number: int
    mask_file: str
    run: bool = True


class BFactors(SQLModel, table=True):  # type: ignore
    bfactor_directory: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.processingJobId")
    number_of_particles: int
    resolution: float


class MotionCorrection(SQLModel, table=True):  # type: ignore
    motionCorrectionId: int = Field(primary_key=True, unique=True)
    dataCollectionId: Optional[int] = Field(
        foreign_key="datacollection.dataCollectionId"
    )
    autoProcProgramId: Optional[int] = Field(
        foreign_key="autoprocprogram.autoProcProgramId"
    )
    imageNumber: Optional[int] = None
    firstFrame: Optional[int] = None
    lastFrame: Optional[int] = None
    dosePerFrame: Optional[float] = None
    doseWeight: Optional[float] = None
    totalMotion: Optional[float] = None
    averageMotionPerFrame: Optional[float] = None
    driftPlotFullPath: Optional[str] = None
    micrographFullPath: Optional[str] = None
    micrographSnapshotFullPath: Optional[str] = None
    patchesUsedX: Optional[int] = None
    patchesUsedY: Optional[int] = None
    fftFullPath: Optional[str] = None
    fftCorrectedFullPath: Optional[str] = None
    comments: Optional[str] = None
    movieId: Optional[int] = Field(foreign_key="movie.movieId")
    auto_proc_program: Optional["AutoProcProgram"] = Relationship(
        back_populates="motion_correction"
    )
    data_collection: Optional["DataCollection"] = Relationship(
        back_populates="motion_correction"
    )
    movie: Optional["Movie"] = Relationship(back_populates="motion_correction")
    ctf: List["CTF"] = Relationship(back_populates="motion_correction")
    particle_picker: List["ParticlePicker"] = Relationship(
        back_populates="motion_correction"
    )
    relative_ice_thickness: List["RelativeIceThickness"] = Relationship(
        back_populates="motion_correction"
    )


class CTF(SQLModel, table=True):  # type: ignore
    ctfId: int = Field(primary_key=True, unique=True)
    motionCorrectionId: Optional[int] = Field(
        foreign_key="motioncorrection.motionCorrectionId"
    )
    autoProcProgramId: Optional[int] = Field(
        foreign_key="autoprocprogram.autoProcProgramId"
    )
    boxSizeX: Optional[float] = None
    boxSizeY: Optional[float] = None
    minResolution: Optional[float] = None
    maxResolution: Optional[float] = None
    minDefocus: Optional[float] = None
    maxDefocus: Optional[float] = None
    defocusStepSize: Optional[float] = None
    astigmatism: Optional[float] = None
    astigmatismAngle: Optional[float] = None
    estimatedResolution: Optional[float] = None
    estimatedDefocus: Optional[float] = None
    amplitudeContrast: Optional[float] = None
    ccValue: Optional[float] = None
    fftTheoreticalFullPath: Optional[str] = None
    iceRingDensity: Optional[float] = None
    comments: Optional[str] = None
    auto_proc_program: Optional["AutoProcProgram"] = Relationship(back_populates="ctf")
    motion_correction: Optional["MotionCorrection"] = Relationship(back_populates="ctf")


class ParticlePicker(SQLModel, table=True):  # type: ignore
    particlePickerId: int = Field(primary_key=True, unique=True)
    programId: Optional[int] = Field(foreign_key="autoprocprogram.autoProcProgramId")
    firstMotionCorrectionId: Optional[int] = Field(
        foreign_key="motioncorrection.motionCorrectionId"
    )
    particlePickingTemplate: Optional[str] = None
    particleDiameter: Optional[float] = None
    numberOfParticles: Optional[int] = None
    summaryImageFullPath: Optional[str] = None
    motion_correction: Optional["MotionCorrection"] = Relationship(
        back_populates="particle_picker"
    )
    auto_proc_program: Optional["AutoProcProgram"] = Relationship(
        back_populates="particle_picker"
    )
    particle_classification_group: List["ParticleClassificationGroup"] = Relationship(
        back_populates="particle_picker"
    )


class Tomogram(SQLModel, table=True):  # type: ignore
    tomogramId: int = Field(primary_key=True, unique=True)
    dataCollectionId: Optional[int] = Field(
        foreign_key="datacollection.dataCollectionId"
    )
    autoProcProgramId: Optional[int] = Field(
        foreign_key="autoprocprogram.autoProcProgramId"
    )
    volumeFile: Optional[str] = None
    stackFile: Optional[str] = None
    sizeX: Optional[int] = None
    sizeY: Optional[int] = None
    sizeZ: Optional[int] = None
    pixelSpacing: Optional[float] = None
    residualErrorMean: Optional[float] = None
    residualErrorSD: Optional[float] = None
    xAxisCorrection: Optional[float] = None
    tiltAngleOffset: Optional[float] = None
    zShift: Optional[float] = None
    fileDirectory: Optional[str] = None
    centralSliceImage: Optional[str] = None
    tomogramMovie: Optional[str] = None
    xyShiftPlot: Optional[str] = None
    projXY: Optional[str] = None
    projXZ: Optional[str] = None
    recordTimeStamp: Optional[datetime] = None
    globalAlignmentQuality: Optional[float] = None
    gridSquareId: Optional[int] = Field(foreign_key="searchmap.id")
    pixelLocationX: Optional[int] = None
    pixelLocationY: Optional[int] = None
    auto_proc_program: Optional["AutoProcProgram"] = Relationship(
        back_populates="tomogram"
    )
    data_collection: Optional["DataCollection"] = Relationship(
        back_populates="tomogram"
    )
    search_map: Optional["SearchMap"] = Relationship(back_populates="tomogram")
    processed_tomogram: List["ProcessedTomogram"] = Relationship(
        back_populates="tomogram"
    )
    tilt_image_alignment: List["TiltImageAlignment"] = Relationship(
        back_populates="tomogram"
    )


class ProcessedTomogram(SQLModel, table=True):  # type: ignore
    processedTomogramId: int = Field(primary_key=True, unique=True)
    tomogramId: int = Field(foreign_key="tomogram.tomogramId")
    filePath: Optional[str] = None
    processingType: Optional[str] = None
    tomogram: Optional["Tomogram"] = Relationship(back_populates="processed_tomogram")


class RelativeIceThickness(SQLModel, table=True):  # type: ignore
    relativeIceThicknessId: int = Field(primary_key=True, unique=True)
    motionCorrectionId: Optional[int] = Field(
        foreign_key="motioncorrection.motionCorrectionId"
    )
    autoProcProgramId: Optional[int] = Field(
        foreign_key="autoprocprogram.autoProcProgramId"
    )
    minimum: Optional[float] = None
    q1: Optional[float] = None
    median: Optional[float] = None
    q3: Optional[float] = None
    maximum: Optional[float] = None
    auto_proc_program: Optional["AutoProcProgram"] = Relationship(
        back_populates="relative_ice_thickness"
    )
    motion_correction: Optional["MotionCorrection"] = Relationship(
        back_populates="relative_ice_thickness"
    )


class TiltImageAlignment(SQLModel, table=True):  # type: ignore
    movieId: int = Field(foreign_key="movie.movieId", primary_key=True)
    tomogramId: int = Field(foreign_key="tomogram.tomogramId", primary_key=True)
    defocusU: Optional[float] = None
    defocusV: Optional[float] = None
    psdFile: Optional[str] = None
    resolution: Optional[float] = None
    fitQuality: Optional[float] = None
    refinedMagnification: Optional[float] = None
    refinedTiltAngle: Optional[float] = None
    refinedTiltAxis: Optional[float] = None
    residualError: Optional[float] = None
    movie: Optional["Movie"] = Relationship(back_populates="tilt_image_alignment")
    tomogram: Optional["Tomogram"] = Relationship(back_populates="tilt_image_alignment")


class ParticleClassificationGroup(SQLModel, table=True):  # type: ignore
    particleClassificationGroupId: int = Field(primary_key=True, unique=True)
    particlePickerId: Optional[int] = Field(
        foreign_key="particlepicker.particlePickerId"
    )
    programId: Optional[int] = Field(foreign_key="autoprocprogram.autoProcProgramId")
    type: Optional[str] = Enum("2D", "3D")
    batchNumber: Optional[int] = None
    numberOfParticlesPerBatch: Optional[int] = None
    numberOfClassesPerBatch: Optional[int] = None
    symmetry: Optional[str] = None
    binnedPixelSize: Optional[float] = None
    particle_picker: Optional["ParticlePicker"] = Relationship(
        back_populates="particle_classification_group"
    )
    auto_proc_program: Optional["AutoProcProgram"] = Relationship(
        back_populates="particle_classification_group"
    )
    particle_classification: List["ParticleClassification"] = Relationship(
        back_populates="particle_classification_group"
    )


class ParticleClassification(SQLModel, table=True):  # type: ignore
    particleClassificationId: int = Field(primary_key=True, unique=True)
    classNumber: Optional[int] = None
    classImageFullPath: Optional[str] = None
    particlesPerClass: Optional[int] = None
    rotationAccuracy: Optional[float] = None
    translationAccuracy: Optional[float] = None
    estimatedResolution: Optional[float] = None
    overallFourierCompleteness: Optional[float] = None
    particleClassificationGroupId: Optional[int] = Field(
        foreign_key="particleclassificationgroup.particleClassificationGroupId"
    )
    classDistribution: Optional[float] = None
    selected: Optional[int] = None
    bFactorFitIntercept: Optional[float] = None
    bFactorFitLinear: Optional[float] = None
    bFactorFitQuadratic: Optional[float] = None
    angularEfficiency: Optional[float] = None
    suggestedTilt: Optional[float] = None
    cryoem_initial_model: List["CryoemInitialModel"] = Relationship(
        back_populates="particle_classification"
    )
    particle_classification_group: Optional["ParticleClassificationGroup"] = (
        Relationship(back_populates="particle_classification")
    )
    bfactor_fit: List["BFactorFit"] = Relationship(
        back_populates="particle_classification"
    )


class BFactorFit(SQLModel, table=True):  # type: ignore
    bFactorFitId: int = Field(primary_key=True, unique=True)
    particleClassificationId: int = Field(
        foreign_key="particleclassification.particleClassificationId"
    )
    resolution: Optional[float] = None
    numberOfParticles: Optional[int] = None
    particleBatchSize: Optional[int] = None
    particle_classification: Optional["ParticleClassification"] = Relationship(
        back_populates="bfactor_fit"
    )


class CryoemInitialModel(SQLModel, table=True):  # type: ignore
    cryoemInitialModelId: int = Field(primary_key=True, unique=True)
    particleClassificationId: int = Field(
        foreign_key="particleclassification.particleClassificationId"
    )
    resolution: Optional[float] = None
    numberOfParticles: Optional[int] = None
    particle_classification: List["ParticleClassification"] = Relationship(
        back_populates="cryoem_initial_model"
    )


"""
FUNCTIONS
"""


def setup(url: str):
    engine = create_engine(url)
    SQLModel.metadata.create_all(engine)


def clear(url: str):
    engine = create_engine(url)
    metadata = sqlalchemy.MetaData()
    metadata.create_all(engine)
    metadata.reflect(engine)
    metadata.drop_all(engine)
