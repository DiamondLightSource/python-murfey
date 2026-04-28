import os
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.gain import Camera, prepare_gain
from murfey.util import secure_path


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_params",
    (  # Has executables | Camera | Suffix | Tag | Return Codes | Exists | Rescale
        # Early exit cases on top
        (False, 1, ".dm4", "", (0, 0, 0), False, True),  # No executables
        (True, 3, ".dm4", "", (0, 0, 0), False, True),  # Falcon camera
        (True, 1, ".mrc", "", (0, 0, 0), False, True),  # Not .dm4 file
        (True, 1, ".dm4", "", (1, 0, 0), False, True),  # 'dm2mrc' failure
        (True, 1, ".dm4", "", (0, 1, 0), False, True),  # 'clip' failure
        (True, 1, ".dm4", "", (0, 0, 1), False, True),  # 'newstack' failure
        # Normal cases
        (True, 1, ".dm4", "20250123T123456", (0, 0, 0), False, True),
        (True, 2, ".dm4", "", (0, 0, 0), False, True),
        (True, 1, ".dm4", "20250123T123456", (0, 0, 0), False, False),
        (True, 2, ".dm4", "", (0, 0, 0), False, False),
        (True, 1, ".dm4", "20250123T123456", (0, 0, 0), True, True),
        (True, 2, ".dm4", "", (0, 0, 0), True, True),
        (True, 1, ".dm4", "20250123T123456", (0, 0, 0), True, False),
        (True, 2, ".dm4", "", (0, 0, 0), True, False),
    ),
)
async def test_prepare_gain(
    mocker: MockerFixture,
    test_params: tuple[bool, int, str, str, tuple[int, int, int], bool, bool],
    tmp_path: Path,
):
    # Unpack test params
    has_executables, camera, suffix, tag, return_codes, exists, rescale = test_params

    # Visit directory
    visit_dir = tmp_path / "data" / "2026" / "visit"

    # Original gain reference
    gain_path = visit_dir / "processing" / f"K3-18480071_Gain_Ref._x1.m1.kv300{suffix}"
    gain_path.parent.mkdir(parents=True, exist_ok=True)
    gain_path.touch(exist_ok=True)

    # Output files
    gain_out = (
        gain_path.parent / f"gain_{tag}.mrc" if tag else gain_path.parent / "gain.mrc"
    )
    if exists:
        gain_out.touch(exist_ok=True)
    gain_out_superres = (
        gain_path.parent / f"gain_{tag}_superres.mrc"
        if tag
        else gain_path.parent / "gain_superres.mrc"
    )
    # Create additional gain paths in a nested directory
    gain_dir = f"gain_{tag}" if tag else "gain"
    gain_path_new = gain_path.parent / gain_dir / gain_path.name
    gain_path_mrc = gain_path_new.with_suffix(".mrc")
    gain_path_superres = gain_path_new.parent / (gain_path_new.name + "_superres.mrc")

    # Dummy executables
    executables = (
        {
            "dm2mrc": mock.ANY,
            "clip": mock.ANY,
            "newstack": mock.ANY,
        }
        if has_executables
        else {}
    )

    # Dummy environment variables
    env = {
        "dummy1": "dummy1",
        "dummy2": "dummy2",
    }

    # Mock the logger to check that expected messages are there
    mock_logger = mocker.patch("murfey.server.gain.logger")

    # Create mocks for the different subprocess calls
    mock_subprocesses = []
    for returncode in return_codes:
        mock_subprocess = MagicMock()
        mock_subprocess.communicate = AsyncMock(return_value=(b"dummy", b"dummy"))
        mock_subprocess.returncode = returncode
        mock_subprocesses.append(mock_subprocess)

    # Patch 'asyncio.create_subprocess_shell'
    mock_shell = mocker.patch(
        "murfey.server.gain.asyncio.create_subprocess_shell",
        new_callable=AsyncMock,
        side_effect=mock_subprocesses,
    )

    # Create the commands that the subprocesses are expected to be called with, in order
    flip = "flipx" if camera == Camera.K3_FLIPX else "flipy"
    commands = (
        [
            f"{executables['dm2mrc']} {gain_path_new} {gain_path_mrc}",
            f"{executables['clip']} {flip} {secure_path(gain_path_mrc)} {secure_path(gain_path_superres) if rescale else secure_path(gain_out)}",
            f"{executables['newstack']} -bin 2 {secure_path(gain_path_superres)} {secure_path(gain_out)}",
        ]
        if has_executables
        else []
    )

    result = await prepare_gain(
        camera=camera,
        gain_path=gain_path,
        executables=executables,
        env=env,
        rescale=rescale,
        tag=tag,
        chmod=0o750,
    )

    # Check early exit cases
    # No executables
    if not has_executables:
        mock_logger.error.assert_called_with(
            "No executables were provided to prepare the gain reference with"
        )
        assert result == (None, None)
    # Falcon camera
    elif camera == Camera.FALCON:
        mock_logger.info.assert_called_with(
            "Gain reference preparation not needed for Falcon detector"
        )
        assert result == (None, None)
    # Not a DM file
    elif suffix != ".dm4":
        assert result == (None, None)
    # 'dm2mrc' fails
    elif return_codes[0]:
        assert mock_shell.call_count == 1
        mock_logger.error.assert_called_with(
            "Error encountered while trying to process the gain reference with 'dm2mrc': \n"
            "dummy"
        )
        assert result == (None, None)
    # 'clip' fails
    elif return_codes[1]:
        assert mock_shell.call_count == 2
        mock_logger.error.assert_called_with(
            "Error encountered while trying to process the gain reference with 'clip': \n"
            "dummy"
        )
        assert result == (None, None)
    # 'newstack' fails
    elif return_codes[2] and rescale:
        assert mock_shell.call_count == 3
        mock_logger.error.assert_called_with(
            "Error encountered while trying to process the gain reference with 'newstack': \n"
            "dummy"
        )
        assert result == (None, None)
    # File already exists
    elif exists:
        assert mock_shell.call_count == 0
        assert result == (gain_out, (gain_out_superres if rescale else gain_out))

    # Check that the expected calls were made
    else:
        # Environment variables were set
        for k, v in env.items():
            assert os.getenv(k) == v
        assert mock_shell.call_count == 3 if rescale else 2
        for i, awaited in enumerate(mock_shell.await_args_list):
            args, _ = awaited
            assert args[0] == commands[i]
        assert result == (gain_out, (gain_out_superres if rescale else gain_out))


@pytest.mark.asyncio
async def test_prepare_eer_gain():
    pass
