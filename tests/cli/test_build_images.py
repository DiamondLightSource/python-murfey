import grp
import os
import sys
from unittest.mock import call, patch

import pytest

from murfey.cli.build_images import run

images = [f"test_image_{n}" for n in range(3)]

# Set defaults of the various flags
def_tags = ["latest"]
def_src = "/home/runner/work/python-murfey/python-murfey"
def_dst = "localhost"
def_uid = os.getuid()
def_gid = os.getgid()
def_gname = grp.getgrgid(os.getgid()).gr_name if hasattr(grp, "getgrgid") else "nogroup"
def_dry_run = False


test_run_params_matrix: tuple[
    tuple[list[str], list[str], str, str, str, str, str, bool]
] = (
    # Images | Tags | Source | Destination | User ID | Group ID | Group Name | Dry Run
    # Default settings
    (images, [], "", "", "", "", "", False),
)


@pytest.mark.parametrize("build_params", test_run_params_matrix)
@patch("murfey.cli.build_images.Path.exists")
@patch("murfey.cli.build_images.run_subprocess")
@patch("murfey.cli.build_images.cleanup")
@patch("murfey.cli.build_images.push_images")
@patch("murfey.cli.build_images.tag_image")
@patch("murfey.cli.build_images.build_image")
def test_run(
    mock_build,
    mock_tag,
    mock_push,
    mock_clean,
    mock_subprocess,
    mock_exists,
    build_params: tuple[list[str], list[str], str, str, str, str, str, bool],
):
    """
    Tests that the function is run with the expected arguments for a given
    combination of flags.
    """

    # Unpack build params
    images, tags, src, dst, uid, gid, gname, dry_run = build_params

    # Set up the command based on what these values are
    build_cmd = [
        "murfey.build_images",
        " ".join(images),
    ]

    # Iterate through flags and add them according to the command
    flags = (
        # 'images' already include by default
        ("--tags", tags),
        ("--source", src),
        ("--destination", dst),
        ("--user-id", uid),
        ("--group-id", gid),
        ("--group-name", gname),
        ("--dry-run", dry_run),
    )
    for flag, value in flags:
        if isinstance(value, list) and value:
            build_cmd.extend([flag, *value])
        if isinstance(value, str) and value:
            build_cmd.extend([flag, value])
        if isinstance(value, bool) and value:
            build_cmd.append(flag)

    # Assign it to the CLI to pass to the function
    sys.argv = build_cmd

    # Mock the check for the existence of the Dockerfiles
    mock_exists.return_value = True

    # Mock the exit code of the subprocesses being run
    mock_subprocess.return_value = 0

    # Mock 'build_image' return values
    image_paths = [
        f"{dst if dst else def_dst}/{image[0]}:{tags[0] if tags else def_tags[0]}"
        for image in images
    ]
    mock_build.side_effect = image_paths

    # Mock all the return values when tagging the images
    all_tags = [
        f"{image.split(':')[0]}:{tag}"
        for image in image_paths
        for tag in (tags if tags else def_tags)
    ]
    mock_tag.side_effect = all_tags

    # Mock the push function
    mock_push.return_value = True

    # Mock the cleanup function
    mock_clean.return_value = True

    # Run the function with the command
    run()

    # Check that the functions were called with the correct flags
    assert mock_build.call_count == len(images)
    expected_calls = (
        call(
            image=image,
            tag=tags[0] if tags else def_tags[0],
            source=src if src else def_src,
            destination=dst if dst else def_dst,
            user_id=uid if uid else def_uid,
            group_id=gid if gid else def_gid,
            groupd_name=gname if gname else def_gname,
            dry_run=dry_run if dry_run else def_dry_run,
        )
        for image in images
    )
    mock_build.assert_has_calls(expected_calls, any_order=True)
