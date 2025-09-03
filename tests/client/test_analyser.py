from __future__ import annotations

import pytest

from murfey.client.analyser import Analyser
from murfey.client.contexts.atlas import AtlasContext
from murfey.client.contexts.clem import CLEMContext
from murfey.client.contexts.spa import SPAModularContext
from murfey.client.contexts.spa_metadata import SPAMetadataContext
from murfey.client.contexts.tomo import TomographyContext
from murfey.client.contexts.tomo_metadata import TomographyMetadataContext
from murfey.util.models import ProcessingParametersSPA, ProcessingParametersTomo

example_files = [
    # Tomography
    ["visit/Position_1_001_0.0_20250715_012434_fractions.tiff", TomographyContext],
    ["visit/Position_1_2_002_3.0_20250715_012434_Fractions.mrc", TomographyContext],
    ["visit/Position_1_2_003_6.0_20250715_012434_EER.eer", TomographyContext],
    ["visit/name1_004_9.0_20250715_012434_fractions.tiff", TomographyContext],
    ["visit/Position_1_[30.0].tiff", TomographyContext],
    ["visit/Position_1.mdoc", TomographyContext],
    ["visit/name1_2.mdoc", TomographyContext],
    # Tomography metadata
    ["visit/Session.dm", TomographyMetadataContext],
    ["visit/SearchMaps/SearchMap.xml", TomographyMetadataContext],
    ["visit/Batch/BatchPositionsList.xml", TomographyMetadataContext],
    ["visit/Thumbnails/file.mrc", TomographyMetadataContext],
    # SPA
    ["visit/FoilHole_01234_fractions.tiff", SPAModularContext],
    ["visit/FoilHole_01234_EER.eer", SPAModularContext],
    # SPA metadata
    ["atlas/atlas.mrc", AtlasContext],
    ["visit/EpuSession.dm", SPAMetadataContext],
    ["visit/Metadata/GridSquare.dm", SPAMetadataContext],
    # CLEM LIF file
    ["visit/images/test_file.lif", CLEMContext],
    # CLEM TIFF files
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12--Z02--C01.tif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12_Lng_LVCC--Z02--C01.tif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Series001--Z00--C00.tif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Series001_Lng_LVCC--Z00--C00.tif",
        CLEMContext,
    ],
    # CLEM TIFF file accompanying metadata
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Position 12.xlif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Position 12_Lng_LVCC.xlif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12/Metadata/Position 12_histo.xlif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12/Metadata/Position 12_Lng_LVCC_histo.xlif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Series001.xlif",
        CLEMContext,
    ],
    [
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Series001_Lng_LVCC.xlif",
        CLEMContext,
    ],
]


@pytest.mark.parametrize("file_and_context", example_files)
def test_find_context(file_and_context, tmp_path):
    # Unpack parametrised variables
    file_name, context = file_and_context

    # Pass the file to the Analyser; add environment as needed
    analyser = Analyser(basepath_local=tmp_path, token="")

    # Check that the results are as expected
    assert analyser._find_context(tmp_path / file_name)
    assert isinstance(analyser._context, context)

    # Checks for the specific workflow contexts
    if isinstance(analyser._context, TomographyContext):
        assert analyser.parameters_model == ProcessingParametersTomo
    if isinstance(analyser._context, SPAModularContext):
        assert analyser.parameters_model == ProcessingParametersSPA


contextless_files = [
    "visit/Position_1_gain.tiff",
    "visit/FoilHole_01234_gain.tiff",
    "visit/file_1.mrc",
    "visit/FoilHole_01234.mrc",
    "visit/FoilHole_01234.jpg",
    "visit/FoilHole_01234.xml",
    "visit/images/test_file.lifext",
    "visit/images/2024_03_14_12_34_56--Project001/Project001.xlef",
    "visit/images/2024_03_14_12_34_56--Project001/Project001.xlef.lock",
    "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12/Position 12_histo.lof",
    "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12/Series001_histo.lof",
]


@pytest.mark.parametrize("bad_file", contextless_files)
def test_ignore_contextless_files(bad_file, tmp_path):
    analyser = Analyser(tmp_path, "")
    assert not analyser._find_context(tmp_path / bad_file)
    assert not analyser._context


def test_analyser_setup_and_stopping(tmp_path):
    analyser = Analyser(tmp_path, "")
    assert analyser.queue.empty()
    analyser.start()
    assert analyser.thread.is_alive()
    analyser.stop()
    assert analyser._halt_thread
    assert not analyser.thread.is_alive()


def test_analyser_tomo_determination(tmp_path):
    tomo_file = tmp_path / "Position_1_[30.0].tiff"
    analyser = Analyser(tmp_path, "")
    analyser.start()
    analyser.queue.put(tomo_file)
    analyser.stop()
    assert analyser._context._acquisition_software == "tomo"


def test_analyser_epu_determination(tmp_path):
    tomo_file = tmp_path / "FoilHole_12345_Data_6789_Fractions.tiff"
    analyser = Analyser(tmp_path, "")
    analyser.start()
    analyser.queue.put(tomo_file)
    analyser.stop()
    assert analyser._context._acquisition_software == "epu"
