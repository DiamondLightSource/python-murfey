from __future__ import annotations

import pytest

from murfey.client.analyser import Analyser
from murfey.client.contexts.spa import SPAModularContext
from murfey.client.contexts.spa_metadata import SPAMetadataContext
from murfey.client.contexts.tomo import TomographyContext
from murfey.client.contexts.tomo_metadata import TomographyMetadataContext
from murfey.util.models import ProcessingParametersSPA, ProcessingParametersTomo

example_files = [
    ["visit/Position_1_001_0.0_20250715_012434_fractions.tiff", TomographyContext],
    ["visit/Position_1_2_002_3.0_20250715_012434_Fractions.mrc", TomographyContext],
    ["visit/Position_1_2_003_6.0_20250715_012434_EER.eer", TomographyContext],
    ["visit/name1_004_9.0_20250715_012434_fractions.tiff", TomographyContext],
    ["visit/Position_1_[30.0].tiff", TomographyContext],
    ["visit/Position_1.mdoc", TomographyContext],
    ["visit/name1_2.mdoc", TomographyContext],
    ["visit/Session.dm", TomographyMetadataContext],
    ["visit/SearchMaps/SearchMap.xml", TomographyMetadataContext],
    ["visit/Batch/BatchPositionsList.xml", TomographyMetadataContext],
    ["visit/Thumbnails/file.mrc", TomographyMetadataContext],
    ["visit/FoilHole_01234_fractions.tiff", SPAModularContext],
    ["atlas/atlas.mrc", SPAMetadataContext],
    ["visit/EpuSession.dm", SPAMetadataContext],
    ["visit/Metadata/GridSquare.dm", SPAMetadataContext],
]


@pytest.mark.parametrize("file_and_context", example_files)
def test_find_context(file_and_context, tmp_path):
    file_name, context = file_and_context

    analyser = Analyser(tmp_path)
    assert analyser._find_context(tmp_path / file_name)
    assert isinstance(analyser._context, context)
    if isinstance(analyser._context, TomographyContext):
        assert analyser.parameters_model == ProcessingParametersTomo
    if isinstance(analyser._context, SPAModularContext):
        assert analyser.parameters_model == ProcessingParametersSPA


contextless_files = [
    "visit/Position_1_gain.tiff",
    "visit/FoilHole_01234_gain.tiff",
    "visit/file_1.mrc",
]


@pytest.mark.parametrize("bad_file", contextless_files)
def test_ignore_contextless_files(bad_file, tmp_path):
    analyser = Analyser(tmp_path)
    assert not analyser._find_context(tmp_path / bad_file)
    assert not analyser._context


def test_analyser_setup_and_stopping(tmp_path):
    analyser = Analyser(tmp_path)
    assert analyser.queue.empty()
    analyser.start()
    assert analyser.thread.is_alive()
    analyser.stop()
    assert analyser._halt_thread
    assert not analyser.thread.is_alive()


def test_analyser_tomo_determination(tmp_path):
    tomo_file = tmp_path / "Position_1_[30.0].tiff"
    analyser = Analyser(tmp_path)
    analyser.start()
    analyser.queue.put(tomo_file)
    analyser.stop()
    assert analyser._context._acquisition_software == "tomo"


def test_analyser_epu_determination(tmp_path):
    tomo_file = tmp_path / "FoilHole_12345_Data_6789.tiff"
    analyser = Analyser(tmp_path)
    analyser.start()
    analyser.queue.put(tomo_file)
    analyser.stop()
    assert analyser._context._acquisition_software == "epu"
