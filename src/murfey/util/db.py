"""
Contains classes that are used to store information on the metadata and status of jobs
of the sessions that Murfey is overseeing, along with the relationships between them.
"""

from typing import List, Optional

import sqlalchemy
from sqlmodel import Field, Relationship, SQLModel, create_engine

"""
GENERAL
"""


class MurfeyUser(SQLModel, table=True):  # type: ignore
    username: str = Field(primary_key=True)
    hashed_password: str


class ClientEnvironment(SQLModel, table=True):  # type: ignore
    client_id: Optional[int] = Field(primary_key=True, unique=True)
    visit: str = Field(default="")
    session_id: Optional[int] = Field(foreign_key="session.id")
    connected: bool
    rsync_instances: List["RsyncInstance"] = Relationship(
        back_populates="client", sa_relationship_kwargs={"cascade": "delete"}
    )


class RsyncInstance(SQLModel, table=True):  # type: ignore
    source: str = Field(primary_key=True)
    destination: str = Field(primary_key=True, default="")
    client_id: int = Field(foreign_key="clientenvironment.client_id", primary_key=True)
    files_transferred: int = Field(default=0)
    files_counted: int = Field(default=0)
    transferring: bool = Field(default=False)
    client: Optional[ClientEnvironment] = Relationship(back_populates="rsync_instances")


class Session(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True)
    name: str

    # CLEM Workflow

    # LIF files collected, if any
    lif_files: List["CLEMLIFFile"] = Relationship(
        back_populates="session",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    # TIFF files collected, if any
    tiff_files: List["CLEMTIFFFile"] = Relationship(
        back_populates="session",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    # Metadata files generated
    metadata_files: List["CLEMImageMetadata"] = Relationship(
        back_populates="session", sa_relationship_kwargs={"cascade": "delete"}
    )
    # Image series associated with this session
    image_series: List["CLEMImageSeries"] = Relationship(
        back_populates="session",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    # Image stacks associated with this session
    image_stacks: List["CLEMImageStack"] = Relationship(
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


"""
CLEM WORKFLOW
"""


class CLEMLIFFile(SQLModel, table=True):  # type: ignore
    """
    Database recording the different LIF files acquired during the CLEM session, as
    well as the different image series stored within them.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    file_path: str = Field(index=True)  # Path to LIF file

    # The CLEM session this series belongs to
    session: Optional["Session"] = Relationship(
        back_populates="lif_files"
    )  # Many to one
    session_id: Optional[int] = Field(
        foreign_key="session.id",
        default=None,
    )

    master_metadata: Optional[str] = Field(
        index=True
    )  # Path to master metadata generated from LIF file

    # Offspring
    child_metadata: List["CLEMImageMetadata"] = Relationship(
        back_populates="parent_lif",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to many
    child_series: List["CLEMImageSeries"] = Relationship(
        back_populates="parent_lif",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to many
    child_stacks: List["CLEMImageStack"] = Relationship(
        back_populates="parent_lif",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to many


class CLEMTIFFFile(SQLModel, table=True):  # type: ignore
    """
    Database to record each raw TIFF file acquired during a CLEM session, which are
    used to create an image stack
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    file_path: str = Field(index=True)  # File path to TIFF file on system

    session: Optional["Session"] = Relationship(
        back_populates="tiff_files"
    )  # Many to one
    session_id: Optional[int] = Field(
        foreign_key="session.id",
        default=None,
    )

    # Metadata associated with this TIFF file
    associated_metadata: Optional["CLEMImageMetadata"] = Relationship(
        back_populates="associated_tiffs",
    )  # Many to one
    metadata_id: Optional[int] = Field(
        foreign_key="clemimagemetadata.id",
        default=None,
    )

    # Image series it contributes to
    child_series: Optional["CLEMImageSeries"] = Relationship(
        back_populates="parent_tiffs"
    )  # Many to one
    series_id: Optional[int] = Field(
        foreign_key="clemimageseries.id",
        default=None,
    )

    # Image stack it contributes to
    child_stack: Optional["CLEMImageStack"] = Relationship(
        back_populates="parent_tiffs"
    )  # Many to one
    stack_id: Optional[int] = Field(
        foreign_key="clemimagestack.id",
        default=None,
    )


class CLEMImageMetadata(SQLModel, table=True):  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)
    file_path: str = Field(index=True)  # Full path to metadata file

    session: Optional["Session"] = Relationship(back_populates="metadata_files")
    session_id: Optional[int] = Field(foreign_key="session.id")  # Many to one

    # The parent LIF file this metadata originates from, if any
    parent_lif: Optional[CLEMLIFFile] = Relationship(
        back_populates="child_metadata",
    )  # Many to one
    parent_lif_id: Optional[int] = Field(
        foreign_key="clemliffile.id",
        default=None,
    )
    # The TIFF files related to this file
    associated_tiffs: List["CLEMTIFFFile"] = Relationship(
        back_populates="associated_metadata",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to many

    # Associated series
    associated_series: Optional["CLEMImageSeries"] = Relationship(
        back_populates="associated_metadata",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to one

    # Associated image stacks
    associated_stacks: List["CLEMImageStack"] = Relationship(
        back_populates="associated_metadata",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to many


class CLEMImageSeries(SQLModel, table=True):  # type: ignore
    """
    Database recording the individual files associated with a series, which are to be
    processed together as a group. These files could stem from a parent LIF file, or
    have been compiled together from individual TIFF files.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    series_name: str = Field(
        index=True
    )  # Name of the series, as determined from the metadata

    session: Optional["Session"] = Relationship(
        back_populates="image_series"
    )  # Many to one
    session_id: Optional[int] = Field(
        foreign_key="session.id", default=None, unique=False
    )

    # The parent LIF file this series originates from, if any
    parent_lif: Optional["CLEMLIFFile"] = Relationship(
        back_populates="child_series",
    )  # Many to one
    parent_lif_id: Optional[int] = Field(
        foreign_key="clemliffile.id",
        default=None,
    )

    # The parent TIFF files used to build up the image stacks in the series, if any
    parent_tiffs: List["CLEMTIFFFile"] = Relationship(
        back_populates="child_series", sa_relationship_kwargs={"cascade": "delete"}
    )  # One to many

    # Metadata file for this series
    associated_metadata: Optional["CLEMImageMetadata"] = Relationship(
        back_populates="associated_series",
    )  # One to one
    metadata_id: Optional[int] = Field(
        foreign_key="clemimagemetadata.id",
        default=None,
    )

    # Databases of the image stacks that comprise this series
    child_stacks: List["CLEMImageStack"] = Relationship(
        back_populates="parent_series",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to many

    # Process checklist for series
    images_aligned: bool = False  # Image stacks aligned to reference image
    rgbs_created: bool = False  # Image stacks all colorised
    composite_created: bool = False  # Composite flattened image created
    composite_image: Optional[str] = None  # Full path to composite image


class CLEMImageStack(SQLModel, table=True):  # type: ignore
    """
    Database to keep track of the processing status of a single image stack.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    file_path: str = Field(index=True)  # Full path to the file
    channel_name: Optional[str] = None  # Color associated with stack

    session: Optional["Session"] = Relationship(
        back_populates="image_stacks"
    )  # Many to one
    session_id: Optional[int] = Field(foreign_key="session.id")

    # LIF file this stack originated from
    parent_lif: Optional["CLEMLIFFile"] = Relationship(
        back_populates="child_stacks",
    )  # Many to one
    parent_lif_id: Optional[int] = Field(foreign_key="clemliffile.id", default=None)

    # TIFF files used to build this stack
    parent_tiffs: List["CLEMTIFFFile"] = Relationship(
        back_populates="child_stack",
        sa_relationship_kwargs={"cascade": "delete"},
    )  # One to many

    # Metadata associated with statck
    associated_metadata: Optional["CLEMImageMetadata"] = Relationship(
        back_populates="associated_stacks",
    )  # Many to one
    metadata_id: Optional[int] = Field(
        foreign_key="clemimagemetadata.id",
        default=None,
    )

    # Image series this image stack belongs to
    parent_series: Optional["CLEMImageSeries"] = Relationship(
        back_populates="child_stacks",
    )  # Many to one
    series_id: Optional[int] = Field(
        foreign_key="clemimageseries.id",
        default=None,
    )

    # Process checklist for each image
    stack_created: bool = False  # Verify that the stack has been created
    image_aligned: bool = False  # Verify that image alignment has been done on stack
    aligned_image: Optional[str] = None  # Full path to aligned image stack
    rgb_created: bool = False  # Verify that rgb image has been created
    rgb_image: Optional[str] = None  # Full path to colorised image stack


"""
TEM SESSION AND PROCESSING WORKFLOW
"""


class TiltSeries(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True)
    tag: str
    rsync_source: str
    session_id: int = Field(foreign_key="session.id")
    tilt_series_length: int = -1
    processing_requested: bool = False
    session: Optional[Session] = Relationship(back_populates="tilt_series")
    tilts: List["Tilt"] = Relationship(
        back_populates="tilt_series", sa_relationship_kwargs={"cascade": "delete"}
    )


class Tilt(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True)
    movie_path: str
    tilt_series_id: int = Field(foreign_key="tiltseries.id")
    motion_corrected: bool = False
    tilt_series: Optional[TiltSeries] = Relationship(back_populates="tilts")


class DataCollectionGroup(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    session_id: int = Field(foreign_key="session.id", primary_key=True)
    tag: str = Field(primary_key=True)
    atlas: str = ""
    sample: Optional[int] = None
    session: Optional[Session] = Relationship(back_populates="data_collection_groups")
    data_collections: List["DataCollection"] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    tomography_preprocessing_parameters: List["TomographyPreprocessingParameters"] = (
        Relationship(
            back_populates="data_collection_group",
            sa_relationship_kwargs={"cascade": "delete"},
        )
    )


class DataCollection(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    tag: str = Field(primary_key=True)
    dcg_id: int = Field(foreign_key="datacollectiongroup.id")
    data_collection_group: Optional[DataCollectionGroup] = Relationship(
        back_populates="data_collections"
    )
    processing_jobs: List["ProcessingJob"] = Relationship(
        back_populates="data_collection", sa_relationship_kwargs={"cascade": "delete"}
    )


class ProcessingJob(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    recipe: str = Field(primary_key=True)
    dc_id: int = Field(foreign_key="datacollection.id")
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
    spa_feedback_parameters: List["SPAFeedbackParameters"] = Relationship(
        back_populates="processing_job", sa_relationship_kwargs={"cascade": "delete"}
    )
    tomography_processing_parameters: List["TomographyProcessingParameters"] = (
        Relationship(
            back_populates="processing_job",
            sa_relationship_kwargs={"cascade": "delete"},
        )
    )
    ctf_parameters: List["CtfParameters"] = Relationship(
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
    pj_id: int = Field(foreign_key="processingjob.id")
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="selection_stash"
    )


class TomographyPreprocessingParameters(SQLModel, table=True):  # type: ignore
    dcg_id: int = Field(primary_key=True, foreign_key="datacollectiongroup.id")
    pixel_size: float
    dose_per_frame: float
    voltage: int
    eer_fractionation_file: Optional[str] = None
    motion_corr_binning: int = 1
    gain_ref: Optional[str] = None
    data_collection_group: Optional[DataCollectionGroup] = Relationship(
        back_populates="tomography_preprocessing_parameters"
    )


class TomographyProcessingParameters(SQLModel, table=True):  # type: ignore
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
    manual_tilt_offset: int
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="tomography_processing_parameters"
    )


class AutoProcProgram(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    pj_id: int = Field(foreign_key="processingjob.id")
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="auto_proc_programs"
    )
    murfey_ids: List["MurfeyLedger"] = Relationship(
        back_populates="auto_proc_program", sa_relationship_kwargs={"cascade": "delete"}
    )


class MurfeyLedger(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(primary_key=True, default=None)
    app_id: int = Field(foreign_key="autoprocprogram.id")
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
    spa_feedback_parameters: Optional["SPAFeedbackParameters"] = Relationship(
        back_populates="murfey_ledger", sa_relationship_kwargs={"cascade": "delete"}
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
    foil_holes: List["FoilHole"] = Relationship(
        back_populates="grid_square", sa_relationship_kwargs={"cascade": "delete"}
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


class Movie(SQLModel, table=True):  # type: ignore
    murfey_id: int = Field(primary_key=True, foreign_key="murfeyledger.id")
    foil_hole_id: int = Field(foreign_key="foilhole.id", nullable=True, default=None)
    path: str
    image_number: int
    tag: str
    preprocessed: bool = False
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="movies")
    foil_hole: Optional[FoilHole] = Relationship(back_populates="movies")


class CtfParameters(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    pj_id: int = Field(foreign_key="processingjob.id")
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


class ParticleSizes(SQLModel, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    pj_id: int = Field(foreign_key="processingjob.id")
    particle_size: float
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="particle_sizes"
    )


class SPARelionParameters(SQLModel, table=True):  # type: ignore
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
    angpix: float
    dose_per_frame: float
    gain_ref: Optional[str]
    voltage: int
    motion_corr_binning: int
    eer_grouping: int
    symmetry: str
    particle_diameter: Optional[float]
    downscale: bool
    do_icebreaker_jobs: bool = True
    boxsize: Optional[int] = 256
    small_boxsize: Optional[int] = 64
    mask_diameter: Optional[float] = 190
    processing_job: Optional[ProcessingJob] = Relationship(
        back_populates="spa_parameters"
    )


class SPAFeedbackParameters(SQLModel, table=True):  # type: ignore
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
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
        back_populates="spa_feedback_parameters"
    )
    murfey_ledger: Optional[MurfeyLedger] = Relationship(
        back_populates="spa_feedback_parameters"
    )


class Class2DParameters(SQLModel, table=True):  # type: ignore
    particles_file: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
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
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    processing_job: Optional[ProcessingJob] = Relationship(back_populates="class2ds")
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="class2ds")


class Class3DParameters(SQLModel, table=True):  # type: ignore
    particles_file: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
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
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    # class3d_parameters: Optional[Class3DParameters] = Relationship(
    #    back_populates="class3ds"
    # )
    processing_job: Optional[ProcessingJob] = Relationship(back_populates="class3ds")
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="class3ds")


class RefineParameters(SQLModel, table=True):  # type: ignore
    refine_dir: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
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
    refine_dir: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
    murfey_id: int = Field(foreign_key="murfeyledger.id")
    processing_job: Optional[ProcessingJob] = Relationship(back_populates="refine3ds")
    murfey_ledger: Optional[MurfeyLedger] = Relationship(back_populates="refine3ds")


class BFactorParameters(SQLModel, table=True):  # type: ignore
    project_dir: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
    batch_size: int
    refined_grp_uuid: int
    refined_class_uuid: int
    class_reference: str
    class_number: int
    mask_file: str
    run: bool = True


class BFactors(SQLModel, table=True):  # type: ignore
    bfactor_directory: str = Field(primary_key=True)
    pj_id: int = Field(primary_key=True, foreign_key="processingjob.id")
    number_of_particles: int
    resolution: float


"""
FUNCTIONS
"""


def setup(url: str):
    engine = create_engine(url)
    SQLModel.metadata.create_all(engine)


def clear(url: str):
    engine = create_engine(url)
    metadata = sqlalchemy.MetaData(engine)
    metadata.reflect()

    metadata.drop_all(engine)
