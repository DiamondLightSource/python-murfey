import datetime
from typing import List, Optional

import sqlalchemy
from sqlmodel import Enum, Field, Relationship, create_engine

from murfey.util import db


class MotionCorrection(db.SQLModel, table=True):  # type: ignore
    motionCorrectionId: int = Field(primary_key=True, unique=True)
    dataCollectionId: Optional[int] = Field(foreign_key="db.DataCollection.id")
    autoProcProgramId: Optional[int] = Field(foreign_key="db.AutoProgProgram.id")
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
    movieId: Optional[int] = Field(foreign_key="Movie.murfey_id")
    auto_proc_program: Optional["db.AutoProcProgram"] = Relationship(
        back_populates="motion_correction"
    )
    data_collection: Optional["db.DataCollection"] = Relationship(
        back_populates="motion_correction"
    )
    movie: Optional["db.Movie"] = Relationship(back_populates="motion_correction")
    ctf: List["CTF"] = Relationship(back_populates="motion_correction")
    particle_picker: List["ParticlePicker"] = Relationship(
        back_populates="motion_correction"
    )
    relative_ice_thickness: List["RelativeIceThickness"] = Relationship(
        back_populates="motion_correction"
    )


class CTF(db.SQLModel, table=True):  # type: ignore
    ctfId: int = Field(primary_key=True, unique=True)
    motionCorrectionId: Optional[int] = Field(
        foreign_key="MotionCorrection.motionCorrectionId"
    )
    autoProcProgramId: Optional[int] = Field(foreign_key="db.AutoProcProgram.id")
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
    comments: Optional[str] = None
    auto_proc_program: Optional["db.AutoProcProgram"] = Relationship(
        back_populates="ctf"
    )
    motion_correction: Optional["MotionCorrection"] = Relationship(back_populates="ctf")


class ParticlePicker(db.SQLModel, table=True):  # type: ignore
    particlePickerId: int = Field(primary_key=True, unique=True)
    programId: Optional[int] = Field(foreign_key="db.AutoProcProgram.autoProcProgramId")
    firstMotionCorrectionId: Optional[int] = Field(
        foreign_key="MotionCorrection.motionCorrectionId"
    )
    particlePickingTemplate: Optional[str] = None
    particleDiameter: Optional[float] = None
    numberOfParticles: Optional[int] = None
    summaryImageFullPath: Optional[str] = None
    motion_correction: Optional["MotionCorrection"] = Relationship(
        back_populates="particle_picker"
    )
    auto_proc_program: Optional["db.AutoProcProgram"] = Relationship(
        back_populates="particle_picker"
    )
    particle_classification_group: List["ParticleClassificationGroup"] = Relationship(
        back_populates="particle_picker"
    )


class Tomogram(db.SQLModel, table=True):  # type: ignore
    tomogramId: int = Field(primary_key=True, unique=True)
    dataCollectionId: Optional[int] = Field(foreign_key="db.DataCollection.id")
    autoProcProgramId: Optional[int] = Field(foreign_key="db.AutoProcProgram.id")
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
    recordTimeStamp: Optional[datetime.datetime] = None
    globalAlignmentQuality: Optional[float] = None
    gridSquareId: Optional[int] = Field(foreign_key="SearchMap.id")
    pixelLocationX: Optional[int] = None
    pixelLocationY: Optional[int] = None
    auto_program_program: Optional["db.AutoProcProgram"] = Relationship(
        back_populates="tomogram"
    )
    data_collection: Optional["db.DataCollection"] = Relationship(
        back_populates="tomogram"
    )
    search_map: Optional["db.SearchMap"] = Relationship(back_populates="tomogram")
    processed_tomogram: List["ProcessedTomogram"] = Relationship(
        back_populates="tomogram"
    )
    tilt_image_alignment: List["TiltImageAlignment"] = Relationship(
        back_populates="tomogram"
    )


class ProcessedTomogram(db.SQLModel, table=True):  # type: ignore
    processedTomogramId: int = Field(primary_key=True, unique=True)
    tomogramId: int = Field(foreign_key="Tomogram.tomogramId")
    filePath: Optional[str] = None
    processingType: Optional[str] = None
    tomogram: Optional["Tomogram"] = Relationship(back_populates="processed_tomogram")


class RelativeIceThickness(db.SQLModel, table=True):  # type: ignore
    relativeIceThicknessId: int = Field(primary_key=True, unique=True)
    motionCorrectionId: Optional[int] = Field(
        foreign_key="MotionCorrection.motionCorrectionId"
    )
    autoProcProgramId: Optional[int] = Field(
        foreign_key="db.AutoProcProgram.autoProcProgramId"
    )
    minimum: Optional[float] = None
    q1: Optional[float] = None
    median: Optional[float] = None
    q3: Optional[float] = None
    maximum: Optional[float] = None
    auto_proc_program: Optional["db.AutoProcProgram"] = Relationship(
        back_populates="relative_ice_thickness"
    )
    motion_correction: Optional["MotionCorrection"] = Relationship(
        back_populates="relative_ice_thickness"
    )


class TiltImageAlignment(db.SQLModel, table=True):  # type: ignore
    movieId: int = Field(foreign_key="db.Movie.murfey_id", primary_key=True)
    tomogramId: int = Field(foreign_key="Tomogram.tomogramId", primary_key=True)
    defocusU: Optional[float] = None
    defocusV: Optional[float] = None
    psdFile: Optional[str] = None
    resolution: Optional[float] = None
    fitQuality: Optional[float] = None
    refinedMagnification: Optional[float] = None
    refinedTiltAngle: Optional[float] = None
    refinedTiltAxis: Optional[float] = None
    residualError: Optional[float] = None
    movie: Optional["db.Movie"] = Relationship(back_populates="tilt_image_alignment")
    tomogram: Optional["Tomogram"] = Relationship(back_populates="tilt_image_alignment")


class ParticleClassificationGroup(db.SQLModel, table=True):  # type: ignore
    particleClassificationGroupId: int = Field(primary_key=True, unique=True)
    particlePickerId: Optional[int] = Field(
        foreign_key="ParticlePicker.particlePickerId"
    )
    programId: Optional[int] = Field(foreign_key="db.AutoProcProgram.autoProcProgramId")
    type: Optional[str] = Enum("2D", "3D")
    batchNumber: Optional[int] = None
    numberOfParticlesPerBatch: Optional[int] = None
    numberOfClassesPerBatch: Optional[int] = None
    symmetry: Optional[str] = None
    binnedPixelSize: Optional[float] = None
    particle_picker: Optional["ParticlePicker"] = Relationship(
        back_populates="particle_classification_group"
    )
    auto_proc_program: Optional["db.AutoProcProgram"] = Relationship(
        back_populates="particle_classification_group"
    )
    particle_classification: List["ParticleClassification"] = Relationship(
        back_populates="particle_classification_group"
    )


class ParticleClassification(db.SQLModel, table=True):  # type: ignore
    particleClassificationId: int = Field(primary_key=True, unique=True)
    classNumber: Optional[int] = None
    classImageFullPath: Optional[str] = None
    particlesPerClass: Optional[int] = None
    rotationAccuracy: Optional[float] = None
    translationAccuracy: Optional[float] = None
    estimatedResolution: Optional[float] = None
    overallFourierCompleteness: Optional[float] = None
    particleClassificationGroupId: Optional[int] = Field(
        foreign_key="ParticleClassificationGroup.particleClassificationGroupId"
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


class BFactorFit(db.SQLModel, table=True):  # type: ignore
    bFactorFitId: int = Field(primary_key=True, unique=True)
    particleClassificationId: int = Field(
        foreign_key="ParticleClassification.particleClassificationId"
    )
    resolution: Optional[float] = None
    numberOfParticles: Optional[int] = None
    particleBatchSize: Optional[int] = None
    particle_classification: Optional["ParticleClassification"] = Relationship(
        back_populates="bfactor_fit"
    )


class CryoemInitialModel(db.SQLModel, table=True):  # type: ignore
    cryoemInitialModelId: int = Field(primary_key=True, unique=True)
    particleClassificationId: int = Field(
        foreign_key="ParticleClassification.particleClassificationId"
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
    db.SQLModel.metadata.create_all(engine)


def clear(url: str):
    engine = create_engine(url)
    metadata = sqlalchemy.MetaData()
    metadata.create_all(engine)
    metadata.reflect(engine)
    metadata.drop_all(engine)
