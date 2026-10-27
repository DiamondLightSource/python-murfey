from __future__ import annotations

from pathlib import Path

from murfey.client.contexts.spa import SPAContext


def _epu_xml(custom_data: dict[str, tuple[str, str]]) -> str:
    # Minimal EPU MicroscopeImage XML, with the CustomData entries in the given order
    entries = "".join(
        "<a:KeyValueOfstringanyType>"
        f"<a:Key>{key}</a:Key>"
        f'<a:Value i:type="b:{value_type}" xmlns:b="http://www.w3.org/2001/XMLSchema">{value}</a:Value>'
        "</a:KeyValueOfstringanyType>"
        for key, (value_type, value) in custom_data.items()
    )
    return (
        '<MicroscopeImage xmlns="http://schemas.datacontract.org/2004/07/Fei.SharedObjects"'
        ' xmlns:i="http://www.w3.org/2001/XMLSchema-instance">'
        '<CustomData xmlns:a="http://schemas.microsoft.com/2003/10/Serialization/Arrays">'
        f"{entries}</CustomData>"
        "<SpatialScale><pixelSize><x><numericValue>1.0732100624855079E-10</numericValue></x>"
        "<y><numericValue>1.0732100624855079E-10</numericValue></y></pixelSize></SpatialScale>"
        "<microscopeData><acquisition><camera>"
        '<Binning xmlns:a="http://schemas.datacontract.org/2004/07/System.Drawing"><a:x>1</a:x><a:y>1</a:y></Binning>'
        "<ExposureTime>15</ExposureTime>"
        '<ReadoutArea xmlns:a="http://schemas.datacontract.org/2004/07/System.Drawing">'
        "<a:height>3838</a:height><a:width>3710</a:width></ReadoutArea>"
        "</camera></acquisition>"
        "<gun><AccelerationVoltage>300000</AccelerationVoltage></gun>"
        "<optics><TemMagnification><NominalMagnification>130000</NominalMagnification></TemMagnification></optics>"
        "</microscopeData></MicroscopeImage>"
    )


def test_spa_gather_metadata_short_custom_data(tmp_path: Path):
    # EPU 1.x and 2.x write only a few CustomData entries, with no C2 aperture name
    metadata_file = tmp_path / "FoilHole_1_Data_2_3_20180906_185308.xml"
    metadata_file.write_text(
        _epu_xml(
            {
                "Dose": ("double", "5.0949700264944667E+21"),
                "PhasePlateUsed": ("boolean", "true"),
                "AppliedDefocus": ("double", "-5E-07"),
            }
        )
    )
    context = SPAContext("epu", tmp_path, {}, "")
    metadata = context.gather_metadata(metadata_file)
    assert metadata["phase_plate"] == 1
    assert metadata["c2aperture"] is None
    assert metadata["total_exposed_dose"] == 50.95
    assert metadata["voltage"] == 300


def test_spa_gather_metadata_reads_custom_data_by_key(tmp_path: Path):
    custom_data = {f"Unrelated{i}": ("string", "true") for i in range(20)}
    custom_data["Aperture[C2].Name"] = ("string", "50")
    custom_data["Dose"] = ("double", "4.2E+21")
    custom_data["PhasePlateUsed"] = ("boolean", "false")
    metadata_file = tmp_path / "FoilHole_1_Data_2_3_20240101_120000.xml"
    metadata_file.write_text(_epu_xml(custom_data))
    context = SPAContext("epu", tmp_path, {}, "")
    metadata = context.gather_metadata(metadata_file)
    assert metadata["phase_plate"] == 0
    assert metadata["c2aperture"] == "50"
    assert metadata["total_exposed_dose"] == 42.0
