import grp
import os
import sys
from unittest.mock import call, patch

import pytest

from murfey.cli.build_images import build_image, push_images, run, tag_image

images = [f"test_image_{n}" for n in range(3)]

# Set defaults of the various flags
def_tags = ["latest"]
def_src = "."
def_dst = "localhost"
def_uid = os.getuid()
def_gid = os.getgid()
def_gname = grp.getgrgid(os.getgid()).gr_name if hasattr(grp, "getgrgid") else "nogroup"
def_dry_run = False


build_image_params_matrix: tuple[
    tuple[list[str], list[str], str, str, int, int, str, bool], ...
] = (
    # Images | Tags | Source | Destination | User ID | Group ID | Group Name | Dry Run
    # Populated flags
    (
        images,
        ["latest", "dev", "1.1.1"],
        "",
        "docker.io",
        12345,
        34567,
        "my-group",
        False,
    ),
    (
        images,
        ["latest", "dev", "1.1.1"],
        "",
        "docker.io",
        12345,
        34567,
        "my-group",
        True,
    ),
)


@pytest.mark.parametrize("build_params", build_image_params_matrix)
@patch("murfey.cli.build_images.Path.exists")
@patch("murfey.cli.build_images.run_subprocess")
def test_build_image(mock_subprocess, mock_exists, build_params):

    # Unpack build params
    images, tags, src, dst, uid, gid, gname, dry_run = build_params

    # Set the return values for 'Path().exists()' and 'run_subprocess'
    mock_exists.return_value = True
    mock_subprocess.return_value = 0

    # Run the command
    built_image = build_image(
        image=images[0],
        tag=tags[0],
        source=src,
        destination=dst,
        user_id=uid,
        group_id=gid,
        group_name=gname,
        dry_run=dry_run,
    )

    # Check that the image path generated is correct
    assert built_image == f"{dst}/{images[0]}:{tags[0]}"


tag_image_params_matrix: tuple[tuple[list[str], list[str], str, bool], ...] = (
    # Images | Tags | Source | Destination | User ID | Group ID | Group Name | Dry Run
    # Populated flags
    (
        images,
        ["latest", "dev", "1.1.1"],
        "docker.io",
        False,
    ),
    (
        images,
        ["latest", "dev", "1.1.1"],
        "docker.io",
        True,
    ),
)


@pytest.mark.parametrize("tag_params", tag_image_params_matrix)
@patch("murfey.cli.build_images.run_subprocess")
def test_tag_image(mock_subprocess, tag_params):

    # Unpack build params
    images, tags, dst, dry_run = tag_params

    # Check that the image path generated is correct
    built_image = f"{dst}/{images[0]}:{tags[0]}"

    # Set the return value for 'run_subprocess'
    mock_subprocess.return_value = 0

    # Run the command
    image_tags = tag_image(
        image_path=built_image,
        tags=tags[1:],
        dry_run=dry_run,
    )

    # Check that the images are tagged correctly
    assert image_tags == [f"{built_image.split(':')[0]}:{tag}" for tag in tags[1:]]


test_run_params_matrix: tuple[
    tuple[list[str], list[str], str, str, str, str, str, bool], ...
] = (
    # Images | Tags | Source | Destination | User ID | Group ID | Group Name | Dry Run
    # Default settings
    (images, [], "", "", "", "", "", False),
    # Populated flags
    (
        images,
        ["latest", "dev", "1.1.1"],
        "",
        "docker.io",
        "12345",
        "34567",
        "my-group",
        False,
    ),
    (
        images,
        ["latest", "dev", "1.1.1"],
        "",
        "docker.io",
        "12345",
        "34567",
        "my-group",
        True,
    ),
)


push_image_params_matrix: tuple[tuple[list[str], list[str], str, bool], ...] = (
    # Images | Tags | Source | Destination | User ID | Group ID | Group Name | Dry Run
    # Populated flags
    (
        images,
        ["latest", "dev", "1.1.1"],
        "docker.io",
        False,
    ),
    (
        images,
        ["latest", "dev", "1.1.1"],
        "docker.io",
        True,
    ),
)


@pytest.mark.parametrize("push_params", push_image_params_matrix)
@patch("murfey.cli.build_images.run_subprocess")
def test_push_images(
    mock_subprocess,
    push_params,
):

    # Unpack test parameters
    images, tags, dst, dry_run = push_params

    # Construct all images to be pushed
    images_to_push = [f"{dst}/{image}:{tag}" for image in images for tag in tags]

    # Mock the subprocess return value
    mock_subprocess.return_value = True

    # Run the function
    result = push_images(
        images=images_to_push,
        dry_run=dry_run,
    )
    assert result


@pytest.mark.parametrize("run_params", test_run_params_matrix)
@patch("murfey.cli.build_images.Path.exists")
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
    run_params: tuple[list[str], list[str], str, str, str, str, str, bool],
):
    """
    Tests that the function is run with the expected arguments for a given
    combination of flags.
    """

    # Unpack build params
    images, tags, src, dst, uid, gid, gname, dry_run = run_params

    # Set up the command based on what these values are
    build_cmd = [
        "murfey.build_images",
        *images,
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

    # Construct images that will be generated at the different stages of the process
    built_images: list[str] = []
    other_tags: list[list[str]] = []
    images_to_push: list[str] = []
    for image in images:
        built_image = (
            f"{dst if dst else def_dst}/{image}:{tags[0] if tags else def_tags[0]}"
        )
        built_images.append(built_image)
        images_to_push.append(built_image)
        new_tags = [
            f"{built_image.split(':')[0]}:{tag}"
            for tag in (tags if tags else def_tags)[1:]
        ]
        other_tags.append(new_tags)
        images_to_push.extend(new_tags)

    # Mock the return values of 'build_image' and 'tag_iamge'
    mock_build.side_effect = built_images
    mock_tag.side_effect = other_tags

    # Mock the push and cleanup functions
    mock_push.return_value = True
    mock_clean.return_value = True

    # Run the function with the command
    run()

    # Check that 'build_image' was called with the correct arguments
    assert mock_build.call_count == len(images)
    expected_build_calls = (
        call(
            image=image,
            tag=tags[0] if tags else def_tags[0],
            source=src if src else def_src,
            destination=dst if dst else def_dst,
            user_id=uid if uid else def_uid,
            group_id=gid if gid else def_gid,
            group_name=gname if gname else def_gname,
            dry_run=dry_run if dry_run else def_dry_run,
        )
        for image in images
    )
    mock_build.assert_has_calls(expected_build_calls, any_order=True)

    # Check that 'tag_image' was called with the correct arguments
    if tags[1:]:
        assert mock_tag.call_count == len(built_images)
        expected_tag_calls = (
            call(
                image_path=image,
                tags=tags[1:],
                dry_run=dry_run if dry_run else def_dry_run,
            )
            for image in built_images
        )
        mock_tag.assert_has_calls(expected_tag_calls, any_order=True)

    # Check that 'push_images' was called with the correct arguments
    mock_push.assert_called_once_with(
        images=images_to_push,
        dry_run=dry_run if dry_run else def_dry_run,
    )

    # Check that 'cleanup' was called correctly
    mock_clean.assert_called_once_with(
        dry_run=dry_run if dry_run else def_dry_run,
    )
