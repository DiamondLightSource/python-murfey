from pathlib import Path

from murfey.client.contexts.sim import SIMContext


def test_sim_context_initialises(tmp_path: Path):
    # Initialise the context with dummy variables
    base_path = tmp_path
    machine_config = {"dummy": "dummy"}
    context = SIMContext(
        "sim",
        basepath=base_path,
        machine_config=machine_config,
        token="dummy",
    )

    assert context._basepath == base_path
    assert context._machine_config == machine_config
    assert context._token == "dummy"
    assert context.name == "SIMContext"


def test_post_transfer(
    tmp_path: Path,
):
    """
    NOTE: This is just a basic test for coverage purposes, and will be rewritten
    as the SIMContext logic evolves and matures.
    """
    # Create a dummy file
    base_path = tmp_path
    visit_dir = base_path / "visit"
    test_file = visit_dir / "raw" / "dummy.txt"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.touch(exist_ok=True)

    # Create other mock variables
    machine_config = {"dummy": "dummy"}

    context = SIMContext(
        "sim",
        basepath=base_path,
        machine_config=machine_config,
        token="dummy",
    )
    assert (
        context.post_transfer(
            transferred_file=test_file,
            environment=None,
        )
        is None
    )
