from typing import List, Optional

import sqlalchemy
from sqlmodel import Field, Relationship, SQLModel, create_engine


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


def setup(url: str):
    engine = create_engine(url)
    SQLModel.metadata.create_all(engine)


def clear(url: str):
    engine = create_engine(url)
    metadata = sqlalchemy.MetaData(engine)
    metadata.reflect()

    metadata.drop_all(engine)
