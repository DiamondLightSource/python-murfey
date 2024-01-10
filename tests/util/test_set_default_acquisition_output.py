import json

from murfey.util import set_default_acquisition_output


def test_set_default_acquisition_output_normal_operation(tmp_path):
    output_dir = tmp_path / "settings.json"
    settings_json = {
        "a": {
            "b": {"data_dir": str(tmp_path)},
            "c": {
                "d": 1,
            },
        }
    }
    with open(output_dir, "w") as sf:
        json.dump(settings_json, sf)
    set_default_acquisition_output(
        tmp_path / "visit", {str(tmp_path / "settings.json"): ["a", "b", "data_dir"]}
    )
    assert (tmp_path / "_murfey_settings.json").is_file()
    with open(output_dir, "r") as sf:
        data = json.load(sf)
    assert data["a"]["b"]["data_dir"] == str(tmp_path / "visit")
    assert data["a"]["c"]["d"] == 1
    with open(output_dir.parent / "_murfey_settings.json", "r") as sf:
        data = json.load(sf)
    assert data["a"]["b"]["data_dir"] == str(tmp_path)
    assert data["a"]["c"]["d"] == 1


def test_set_default_acquisition_output_no_file_copy(tmp_path):
    output_dir = tmp_path / "settings.json"
    settings_json = {
        "a": {
            "b": {"data_dir": str(tmp_path)},
            "c": {
                "d": 1,
            },
        }
    }
    with open(output_dir, "w") as sf:
        json.dump(settings_json, sf)
    set_default_acquisition_output(
        tmp_path / "visit",
        {str(tmp_path / "settings.json"): ["a", "b", "data_dir"]},
        safe=False,
    )
    assert not (tmp_path / "_murfey_settings.json").is_file()
    with open(output_dir, "r") as sf:
        data = json.load(sf)
    assert data["a"]["b"]["data_dir"] == str(tmp_path / "visit")
    assert data["a"]["c"]["d"] == 1
