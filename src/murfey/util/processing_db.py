import datetime
from typing import List, Optional

import sqlalchemy
from sqlmodel import Enum, Field, Relationship, create_engine

from murfey.util.db import (
    AutoProcProgram as AutoProcProgramOrig,
    DataCollection as DataCollectionOrig,
    DataCollectionGroup as DataCollectionGroupOrig,
    FoilHole as FoilHoleOrig,
    GridSquare as GridSquareOrig,
    Movie as MovieOrig,
    SearchMap as SearchMapOrig,
    SQLModel,
)


class DataCollectionGroup(DataCollectionGroupOrig):
    grid_squares: List["GridSquare"] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )
    search_maps: List["SearchMap"] = Relationship(
        back_populates="data_collection_group",
        sa_relationship_kwargs={"cascade": "delete"},
    )


class DataCollection(DataCollectionOrig):
    MotionCorrection: List["MotionCorrection"] = Relationship(
        back_populates="DataCollection"
    )
    Tomogram: List["Tomogram"] = Relationship(back_populates="DataCollection")


class AutoProcProgram(AutoProcProgramOrig):
    MotionCorrection: List["MotionCorrection"] = Relationship(
        back_populates="DataCollection"
    )
    Tomogram: List["Tomogram"] = Relationship(back_populates="AutoProcProgram")
    CTF: List["CTF"] = Relationship(back_populates="AutoProcProgram")
    ParticlePicker: List["ParticlePicker"] = Relationship(
        back_populates="AutoProcProgram"
    )
    RelativeIceThickness: List["RelativeIceThickness"] = Relationship(
        back_populates="AutoProcProgram"
    )
    ParticleClassificationGroup: List["ParticleClassificationGroup"] = Relationship(
        back_populates="AutoProcProgram"
    )


class GridSquare(GridSquareOrig):
    atlas_id: Optional[int] = Field(foreign_key="datacollectiongroup.id")
    scaled_pixel_size: Optional[float] = None
    pixel_location_x: Optional[int] = None
    pixel_location_y: Optional[int] = None
    height: Optional[int] = None
    width: Optional[int] = None
    angle: Optional[float] = None
    quality_indicator: Optional[float] = None
    data_collection_group: Optional["DataCollectionGroup"] = Relationship(
        back_populates="grid_squares"
    )


class FoilHole(FoilHoleOrig):
    scaled_pixel_size: Optional[float] = None
    pixel_location_x: Optional[int] = None
    pixel_location_y: Optional[int] = None
    diameter: Optional[int] = None
    quality_indicator: Optional[float] = None


class SearchMap(SearchMapOrig):
    atlas_id: Optional[int] = Field(foreign_key="datacollectiongroup.id")
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
    Tomogram: List["Tomogram"] = Relationship(back_populates="SearchMap")


class Movie(MovieOrig):
    MotionCorrection: List["MotionCorrection"] = Relationship(back_populates="Movie")
    TiltImageAlignment: List["TiltImageAlignment"] = Relationship(
        back_populates="Movie"
    )


class MotionCorrection(SQLModel, table=True):  # type: ignore
    motionCorrectionId: int = Field(primary_key=True, unique=True)
    dataCollectionId: Optional[int] = Field(foreign_key="DataCollection.id")
    autoProcProgramId: Optional[int] = Field(foreign_key="AutoProcProgram.id")
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
    AutoProcProgram: Optional["AutoProcProgram"] = Relationship(
        back_populates="MotionCorrection"
    )
    DataCollection: Optional["DataCollection"] = Relationship(
        back_populates="MotionCorrection"
    )
    Movie: Optional["Movie"] = Relationship(back_populates="MotionCorrection")
    CTF: List["CTF"] = Relationship(back_populates="MotionCorrection")
    ParticlePicker: List["ParticlePicker"] = Relationship(
        back_populates="MotionCorrection"
    )
    RelativeIceThickness: List["RelativeIceThickness"] = Relationship(
        back_populates="MotionCorrection"
    )


class CTF(SQLModel, table=True):  # type: ignore
    ctfId: int = Field(primary_key=True, unique=True)
    motionCorrectionId: Optional[int] = Field(
        foreign_key="MotionCorrection.motionCorrectionId"
    )
    autoProcProgramId: Optional[int] = Field(foreign_key="AutoProcProgram.id")
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
    AutoProcProgram: Optional["AutoProcProgram"] = Relationship(back_populates="CTF")
    MotionCorrection: Optional["MotionCorrection"] = Relationship(back_populates="CTF")


class ParticlePicker(SQLModel, table=True):  # type: ignore
    particlePickerId: int = Field(primary_key=True, unique=True)
    programId: Optional[int] = Field(foreign_key="AutoProcProgram.autoProcProgramId")
    firstMotionCorrectionId: Optional[int] = Field(
        foreign_key="MotionCorrection.motionCorrectionId"
    )
    particlePickingTemplate: Optional[str] = None
    particleDiameter: Optional[float] = None
    numberOfParticles: Optional[int] = None
    summaryImageFullPath: Optional[str] = None
    MotionCorrection: Optional["MotionCorrection"] = Relationship(
        back_populates="ParticlePicker"
    )
    AutoProcProgram: Optional["AutoProcProgram"] = Relationship(
        back_populates="ParticlePicker"
    )
    ParticleClassificationGroup: List["ParticleClassificationGroup"] = Relationship(
        back_populates="ParticlePicker"
    )


class Tomogram(SQLModel, table=True):  # type: ignore
    tomogramId: int = Field(primary_key=True, unique=True)
    dataCollectionId: Optional[int] = Field(foreign_key="DataCollection.id")
    autoProcProgramId: Optional[int] = Field(foreign_key="AutoProcProgram.id")
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
    AutoProcProgram: Optional["AutoProcProgram"] = Relationship(
        back_populates="Tomogram"
    )
    DataCollection: Optional["DataCollection"] = Relationship(back_populates="Tomogram")
    SearchMap: Optional["SearchMap"] = Relationship(back_populates="Tomogram")
    ProcessedTomogram: List["ProcessedTomogram"] = Relationship(
        back_populates="Tomogram"
    )
    TiltImageAlignment: List["TiltImageAlignment"] = Relationship(
        back_populates="Tomogram"
    )


class ProcessedTomogram(SQLModel, table=True):  # type: ignore
    processedTomogramId: int = Field(primary_key=True, unique=True)
    tomogramId: int = Field(foreign_key="Tomogram.tomogramId")
    filePath: Optional[str] = None
    processingType: Optional[str] = None
    Tomogram: Optional["Tomogram"] = Relationship(back_populates="ProcessedTomogram")


class RelativeIceThickness(SQLModel, table=True):  # type: ignore
    relativeIceThicknessId: int = Field(primary_key=True, unique=True)
    motionCorrectionId: Optional[int] = Field(
        foreign_key="MotionCorrection.motionCorrectionId"
    )
    autoProcProgramId: Optional[int] = Field(
        foreign_key="AutoProcProgram.autoProcProgramId"
    )
    minimum: Optional[float] = None
    q1: Optional[float] = None
    median: Optional[float] = None
    q3: Optional[float] = None
    maximum: Optional[float] = None
    AutoProcProgram: Optional["AutoProcProgram"] = Relationship(
        back_populates="RelativeIceThickness"
    )
    MotionCorrection: Optional["MotionCorrection"] = Relationship(
        back_populates="RelativeIceThickness"
    )


class TiltImageAlignment(SQLModel, table=True):  # type: ignore
    movieId: int = Field(foreign_key="Movie.murfey_id", primary_key=True)
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
    Movie: Optional["Movie"] = Relationship(back_populates="TiltImageAlignment")
    Tomogram: Optional["Tomogram"] = Relationship(back_populates="TiltImageAlignment")


class ParticleClassificationGroup(SQLModel, table=True):  # type: ignore
    particleClassificationGroupId: int = Field(primary_key=True, unique=True)
    particlePickerId: Optional[int] = Field(
        foreign_key="ParticlePicker.particlePickerId"
    )
    programId: Optional[int] = Field(foreign_key="AutoProcProgram.autoProcProgramId")
    type: Optional[str] = Enum("2D", "3D")
    batchNumber: Optional[int] = None
    numberOfParticlesPerBatch: Optional[int] = None
    numberOfClassesPerBatch: Optional[int] = None
    symmetry: Optional[str] = None
    binnedPixelSize: Optional[float] = None
    ParticlePicker: Optional["ParticlePicker"] = Relationship(
        back_populates="ParticleClassificationGroup"
    )
    AutoProcProgram: Optional["AutoProcProgram"] = Relationship(
        back_populates="ParticleClassificationGroup"
    )
    ParticleClassification: List["ParticleClassification"] = Relationship(
        back_populates="ParticleClassificationGroup"
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
        foreign_key="ParticleClassificationGroup.particleClassificationGroupId"
    )
    classDistribution: Optional[float] = None
    selected: Optional[int] = None
    bFactorFitIntercept: Optional[float] = None
    bFactorFitLinear: Optional[float] = None
    bFactorFitQuadratic: Optional[float] = None
    angularEfficiency: Optional[float] = None
    suggestedTilt: Optional[float] = None
    CryoemInitialModel: List["CryoemInitialModel"] = Relationship(
        back_populates="ParticleClassification"
    )
    ParticleClassificationGroup: Optional["ParticleClassificationGroup"] = Relationship(
        back_populates="ParticleClassification"
    )
    BFactorFit: List["BFactorFit"] = Relationship(
        back_populates="ParticleClassification"
    )


class BFactorFit(SQLModel, table=True):  # type: ignore
    bFactorFitId: int = Field(primary_key=True, unique=True)
    particleClassificationId: int = Field(
        foreign_key="ParticleClassification.particleClassificationId"
    )
    resolution: Optional[float] = None
    numberOfParticles: Optional[int] = None
    particleBatchSize: Optional[int] = None
    ParticleClassification: Optional["ParticleClassification"] = Relationship(
        back_populates="BFactorFit"
    )


class CryoemInitialModel(SQLModel, table=True):  # type: ignore
    cryoemInitialModelId: int = Field(primary_key=True, unique=True)
    particleClassificationId: int = Field(
        foreign_key="ParticleClassification.particleClassificationId"
    )
    resolution: Optional[float] = None
    numberOfParticles: Optional[int] = None
    ParticleClassification: List["ParticleClassification"] = Relationship(
        back_populates="CryoemInitialModel"
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
