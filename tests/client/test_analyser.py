from __future__ import annotations

from pathlib import Path
from unittest.mock import mock_open

import pytest
from pytest_mock import MockerFixture

from murfey.client.analyser import Analyser
from murfey.util.models import ProcessingParametersSPA, ProcessingParametersTomo

example_files = {
    "CLEMContext": [
        # CLEM LIF file
        "visit/images/test_file.lif",
        # CLEM TIFF files
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12--Z02--C01.tif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12_Lng_LVCC--Z02--C01.tif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Series001--Z00--C00.tif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Series001_Lng_LVCC--Z00--C00.tif",
        # CLEM TIFF file accompanying metadata
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Position 12.xlif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Position 12_Lng_LVCC.xlif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12/Metadata/Position 12_histo.xlif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Position 12/Metadata/Position 12_Lng_LVCC_histo.xlif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Series001.xlif",
        "visit/images/2024_03_14_12_34_56--Project001/grid1/Metadata/Series001_Lng_LVCC.xlif",
    ],
    "FIBContext": [
        # FIB Autotem files
        "visit/autotem/visit/ProjectData.dat",
        "visit/autotem/visit/Sites/Lamella/SetupImages/Preparation.tif",
        "visit/autotem/visit/Sites/Lamella (2)//DCImages/DCM_2026-03-09-23-45-40.926/2026-03-09-23-48-43-Finer-Milling-dc_rescan-image-.png",
        # FIB Maps files
        "visit/maps/visit/EMproject.emxml",
        "visit/maps/visit/LayersData/Layer/Electron Snapshot/Electron Snapshot.tiff",
        "visit/maps/visit/LayersData/Layer/Electron Snapshot (2)/Electron Snapshot (2).tiff",
    ],
    "SIMContext": [
        # Bright field
        "visit/raw/CtrlApr_G2/20260703_132856_CtrlApr_G2_F3A_BF",
        # Fluorescent
        "visit/raw/SR002_G1/20260707_112417_SR002G1_F1F_BR",
        "visit/raw/SR002_G1/20260707_112417_SR002G1_F1F_BFR",
        "visit/raw/44drug_G2/20260703_114348_44drug_G2_E2DR_GR",
        "visit/raw/44drug_G2/20260703_113142_44drug_G2_E2DR_GFR",
        "visit/raw/SR002_G1/20260707_112417_SR002G1_F1F_BR_FL",
        "visit/raw/SR002_G1/20260707_112417_SR002G1_F1F_BFR_FL",
        "visit/raw/44drug_G2/20260703_114348_44drug_G2_E2DR_GR_FL",
        "visit/raw/44drug_G2/20260703_113142_44drug_G2_E2DR_GFR_FL",
    ],
    "SXTContext": [
        "visit/tomo__tag_ROI10_area1_angle-60to60@1.5_1sec_251p.txrm",
        "visit/X-ray_mosaic_ROI2.xrm",
    ],
    "AtlasContext": [
        "atlas/atlas.mrc",
    ],
    "TomographyContext": [
        "visit/Position_1_001_0.0_20250715_012434_fractions.tiff",
        "visit/Position_1_2_002_3.0_20250715_012434_Fractions.mrc",
        "visit/Position_1_2_003_6.0_20250715_012434_EER.eer",
        "visit/name1_004_9.0_20250715_012434_fractions.tiff",
        "visit/Position_1_[30.0].tiff",
        "visit/Position_1.mdoc",
        "visit/name1_2.mdoc",
    ],
    "TomographyMetadataContext": [
        "visit/Session.dm",
        "visit/SearchMaps/SearchMap.xml",
        "visit/Batch/BatchPositionsList.xml",
        "visit/Thumbnails/file.mrc",
    ],
    "SPAContext": [
        "visit/FoilHole_01234_fractions.tiff",
        "visit/FoilHole_01234_EER.eer",
    ],
    "SPAMetadataContext": [
        "visit/EpuSession.dm",
        "visit/Metadata/GridSquare.dm",
    ],
}


@pytest.mark.parametrize(
    "test_file",
    [
        file
        for file_list in example_files.values()
        for file in file_list
        for suffix in (".mrc", ".tiff", ".tif", ".eer", ".mdoc")
        if file.endswith(suffix)
    ],
)
def test_find_extension(
    mocker: MockerFixture,
    test_file: str,
    tmp_path: Path,
):
    # Mock the functions used to open a .mdoc file to return a dummy file path
    m = mock_open(read_data="dummy data")
    mocker.patch("murfey.client.analyser.open", m)
    mocker.patch(
        "murfey.client.analyser.get_block",
        return_value={"SubFramePath": "/path/to/test_file.tiff"},
    )

    # Pass the file to the function, and check the outputs are as expected
    analyser = Analyser(basepath_local=tmp_path, token="")
    assert analyser._find_extension(tmp_path / test_file)
    if not test_file.endswith(".mdoc"):
        assert test_file.endswith(analyser._extension)
    else:
        assert analyser._extension == ".tiff"


@pytest.mark.parametrize(
    "file_and_context",
    [
        [file, context]
        for context, file_list in example_files.items()
        for file in file_list
    ],
)
def test_find_context(file_and_context, tmp_path):
    # Unpack parametrised variables
    file_name, context = file_and_context

    # Set up the Analyser
    analyser = Analyser(basepath_local=tmp_path, token="")

    # Pass the file to the function, and check that outputs are as expected
    assert analyser._find_context(tmp_path / file_name)
    assert analyser._context is not None and context in str(analyser._context)

    # Additional checks for specific contexts
    if analyser._context is not None and analyser._context.name == "TomographyContext":
        assert analyser.parameters_model == ProcessingParametersTomo
    if analyser._context is not None and analyser._context.name == "SPAContext":
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


@pytest.mark.parametrize("test_file", contextless_files)
def test_analyse_no_context(
    test_file: str,
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Mock the 'post_transfer' class function
    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    spy_find_context = mocker.spy(Analyser, "_find_context")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "")
    analyser._analyse(tmp_path / test_file)

    # "_find_context" should be called
    assert spy_find_context.call_count == 1

    # "post_transfer" should not be called
    mock_post_transfer.assert_not_called()


def test_analyse_clem(
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Gather example files related to the CLEM workflow
    test_files = [
        file
        for context, file_list in example_files.items()
        for file in file_list
        if context == "CLEMContext" and not file.endswith(".lif")
    ]

    # Mock the 'post_transfer' class function
    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    spy_find_context = mocker.spy(Analyser, "_find_context")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "")
    for file in test_files:
        analyser._analyse(tmp_path / file)

    # "_find_context" should be called only once
    assert spy_find_context.call_count == 1
    assert analyser._context is not None and "CLEMContext" in str(analyser._context)

    # "_post_transfer" should be called on every one of these files
    assert mock_post_transfer.call_count == len(test_files)


@pytest.mark.parametrize(
    "test_params",
    # Test the "autotem" and "maps" workflows separately
    [
        [software, [file for file in file_list if software in file]]
        for software in ("autotem", "maps")
        for context, file_list in example_files.items()
        if context == "FIBContext"
    ],
)
def test_analyse_fib(
    mocker: MockerFixture,
    test_params: tuple[str, list[str]],
    tmp_path: Path,
):
    # Unpack test params
    software, test_files = test_params

    # Mock the 'post_transfer' class function
    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    spy_find_context = mocker.spy(Analyser, "_find_context")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "")
    for file in test_files:
        analyser._analyse(tmp_path / file)

    # "_find_context" should be called only once
    assert spy_find_context.call_count == 1
    assert analyser._context is not None and "FIBContext" in str(analyser._context)
    assert analyser._context._acquisition_software == software

    # "_post_transfer" should be called on every one of these files
    assert mock_post_transfer.call_count == len(test_files)


def test_analyse_sim(
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Load the example files corresponding to the SIM workflow
    test_files = [
        file
        for context, file_list in example_files.items()
        for file in file_list
        if context == "SIMContext"
    ]

    # Mock the 'post_transfer' class function
    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    spy_find_context = mocker.spy(Analyser, "_find_context")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "")
    for file in test_files:
        analyser._analyse(tmp_path / file)

    # "_find_context" should be called only once
    assert spy_find_context.call_count == 1
    assert analyser._context is not None and "SIMContext" in str(analyser._context)

    # "_post_transfer" should be called on every one of these files
    assert mock_post_transfer.call_count == len(test_files)


def test_analyse_sxt(
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Load the example files corresponding to the SXT workflow
    test_files = [
        file
        for context, file_list in example_files.items()
        for file in file_list
        if context == "SXTContext"
    ]

    # Mock the 'post_transfer' class function
    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    spy_find_context = mocker.spy(Analyser, "_find_context")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "")
    for file in test_files:
        analyser._analyse(tmp_path / file)

    # "_find_context" should be called only once
    assert spy_find_context.call_count == 1
    assert analyser._context is not None and "SXTContext" in str(analyser._context)

    # "_post_transfer" should be called on every one of these files
    assert mock_post_transfer.call_count == len(test_files)


@pytest.mark.parametrize(
    "context_to_test",
    [
        "SPAMetadataContext",
        "TomographyMetadataContext",
    ],
)
def test_analyse_limited(
    mocker: MockerFixture,
    context_to_test: str,
    tmp_path: Path,
):
    # Load example files related to the CLEM
    test_files = [
        file
        for context, file_list in example_files.items()
        for file in file_list
        if context in context_to_test
    ]

    # Mock the 'post_transfer' class function
    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "", limited=True)
    for file in test_files:
        analyser._analyse(tmp_path / file)

    # "_find_context" should be called only once
    assert analyser._context is not None and analyser._context.name == context_to_test

    # "_post_transfer" should be called on every one of these files
    assert mock_post_transfer.call_count == len(test_files)


@pytest.mark.parametrize(
    "context_to_test",
    [
        "AtlasContext",
        "CLEMContext",
        "FIBContext",
        "SPAMetadataContext",
        "SXTContext",
        "TomographyMetadataContext",
    ],
)
def test_analyse_generic(
    mocker: MockerFixture,
    context_to_test: str,
    tmp_path: Path,
):
    """
    Tests the Contexts which has straightforward processing logic.
    """
    test_files = [
        file
        for context, file_list in example_files.items()
        for file in file_list
        if context == context_to_test
    ]

    # Set up mocks and spies
    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    spy_find_context = mocker.spy(Analyser, "_find_context")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "", force_mdoc_metadata=True)
    for file in test_files:
        analyser._analyse(tmp_path / file)

    # "_find_context" should be called once
    assert spy_find_context.call_count == 1

    # Context should be set
    assert analyser._context is not None and analyser._context.name == context_to_test

    # "post_transfer" should be called on all files
    assert mock_post_transfer.call_count == len(test_files)


@pytest.mark.parametrize("has_extension", [True, False])
def test_analyse_spa(
    mocker: MockerFixture,
    has_extension: bool,
    tmp_path: Path,
):
    test_files = [
        file
        for context, file_list in example_files.items()
        for file in file_list
        if context == "SPAContext"
    ]

    # Set up mocks and spies
    mock_metadata = {
        "dummy": "dummy",
    }
    if has_extension:
        mock_metadata["file_extension"] = ".tiff"

    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    mock_gather_metadata = mocker.patch(
        "murfey.client.contexts.spa.SPAContext.gather_metadata",
        return_value=mock_metadata,
    )
    mock_notify = mocker.patch.object(Analyser, "notify")
    spy_find_context = mocker.spy(Analyser, "_find_context")
    spy_find_extension = mocker.spy(Analyser, "_find_extension")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "")
    for file in test_files:
        analyser._analyse(tmp_path / file)

    assert spy_find_context.call_count == 1
    assert analyser._context is not None and analyser._context.name == "SPAContext"
    mock_post_transfer.assert_called()

    assert spy_find_extension.call_count > 0
    assert analyser._extension == ".tiff"
    mock_gather_metadata.assert_called()
    mock_notify.assert_called_with(
        {
            "dummy": "dummy",
            "file_extension": analyser._extension,
            "acquisition_software": analyser._context._acquisition_software,
        }
    )


@pytest.mark.parametrize("has_extension", [True, False])
def test_analyse_tomo(
    mocker: MockerFixture,
    has_extension: bool,
    tmp_path: Path,
):
    test_files = [
        file
        for context, file_list in example_files.items()
        for file in file_list
        if context == "TomographyContext"
    ]

    # Set up mocks and spies
    mock_metadata = {
        "dummy": "dummy",
    }
    if has_extension:
        mock_metadata["file_extension"] = ".tiff"

    mock_post_transfer = mocker.patch.object(Analyser, "post_transfer")
    mock_gather_metadata = mocker.patch(
        "murfey.client.contexts.tomo.TomographyContext.gather_metadata",
        return_value=mock_metadata,
    )
    mock_notify = mocker.patch.object(Analyser, "notify")
    spy_find_context = mocker.spy(Analyser, "_find_context")
    spy_find_extension = mocker.spy(Analyser, "_find_extension")

    # Initialise the Analyser
    analyser = Analyser(tmp_path, "")
    for file in test_files:
        analyser._analyse(tmp_path / file)

    assert spy_find_context.call_count == 1
    assert (
        analyser._context is not None and analyser._context.name == "TomographyContext"
    )
    mock_post_transfer.assert_called()

    assert spy_find_extension.call_count > 0
    assert analyser._extension == ".tiff"
    mock_gather_metadata.assert_called()
    mock_notify.assert_called_with(
        {
            "dummy": "dummy",
            "file_extension": analyser._extension,
            "acquisition_software": analyser._context._acquisition_software,
        }
    )
