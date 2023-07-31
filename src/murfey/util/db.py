from typing import List, Optional

from sqlmodel import Field, Relationship, SQLModel, create_engine


class ClientEnvironment(SQLModel, table=True):  # type: ignore
    client_id: Optional[int] = Field(primary_key=True, unique=True)
    visit: str = Field(default="")
    connected: bool
    rsync_instances: List["RsyncInstance"] = Relationship(
        back_populates="client", sa_relationship_kwargs={"cascade": "delete"}
    )
    tilt_series: List["TiltSeries"] = Relationship(
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


class TiltSeries(SQLModel, table=True):  # type: ignore
    tag: str = Field(primary_key=True)
    client_id: int = Field(foreign_key="clientenvironment.client_id")
    auto_proc_program_id: int = Field(foreign_key="autoprocprogram.id")
    complete: bool = False
    processing_requested: bool = False
    client: Optional[ClientEnvironment] = Relationship(back_populates="tilt_series")
    tilts: List["Tilt"] = Relationship(
        back_populates="tilt_series", sa_relationship_kwargs={"cascade": "delete"}
    )


class Tilt(SQLModel, table=True):  # type: ignore
    movie_path: str = Field(primary_key=True)
    tilt_series_tag: str = Field(foreign_key="tiltseries.tag")
    motion_corrected: bool = False
    tilt_series: Optional[TiltSeries] = Relationship(back_populates="tilts")


class DataCollectionGroup(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    client: int = Field(primary_key=True)
    tag: str


class DataCollection(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    client: int = Field(primary_key=True)
    tag: str
    dcg_id: int = Field(foreign_key="datacollectiongroup.id")


class ProcessingJob(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    recipe: str = Field(primary_key=True)
    dc_id: int = Field(foreign_key="datacollection.id")


class PreprocessStash(SQLModel, table=True):  # type: ignore
    file_path: str = Field(primary_key=True)
    client_id: int = Field(primary_key=True, foreign_key="clientenvironment.client_id")
    image_number: int
    mc_uuid: int
    mrc_out: str


class TomographyProcessingParameters(SQLModel, table=True):  # type: ignore
    client_id: int = Field(primary_key=True, foreign_key="clientenvironment.client_id")
    pixel_size: float
    manual_tilt_offset: int


class AutoProcProgram(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    pj_id: int = Field(foreign_key="processingjob.id")


class CtfParameters(SQLModel, table=True):  # type: ignore
    micrographs_file: str = Field(primary_key=True)
    coord_list_file: str
    extract_file: str
    ctf_image: str
    ctf_max_resolution: float
    ctf_figure_of_merit: float
    defocus_u: float
    defocus_v: float
    defocus_angle: float


class ParticleSizes(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    particle_size: float


class SPARelionParameters(SQLModel, table=True):  # type: ignore
    client_id: int = Field(primary_key=True, foreign_key="clientenvironment.client_id")
    angpix: float
    dose_per_frame: float
    gain_ref: str
    voltage: int
    motion_corr_binning: int
    eer_grouping: int
    symmetry: str
    particle_diameter: float
    downscale: bool
    do_icebreaker_jobs: bool = True
    boxsize: int = 256
    small_boxsize: int = 64
    mask_diameter: float = 190


class SPAFeedbackParameters(SQLModel, table=True):  # type: ignore
    id: int = Field(primary_key=True, unique=True)
    estimate_particle_diameter: bool = True
    hold_class2d: bool = False
    hold_class3d: bool = False
    class_selection_score: float
    star_combination_job: int
    initial_model: str
    next_job: int


class Class2DParameters(SQLModel, table=True):  # type: ignore
    particles_file: str = Field(primary_key=True, unique=True)
    class2d_dir: str
    batch_size: int


class Class3DParameters(SQLModel, table=True):  # type: ignore
    particles_file: str = Field(primary_key=True, unique=True)
    class3d_dir: str
    batch_size: int


def setup(url: str):
    engine = create_engine(url)
    SQLModel.metadata.create_all(engine)
