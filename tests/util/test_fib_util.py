import pytest

from murfey.util.fib import number_from_name


@pytest.mark.parametrize(
    "test_params",
    (  # File name | Expected number
        # AutoTEM examples
        ("Lamella", 1),
        ("Lamella (2)", 2),
        ("Lamella (12)", 12),
        # Maps examples
        ("Electron Snapshot", 1),
        ("Electron Snapshot (3)", 3),
        ("Electron Snapshot (21)", 21),
        # Waffle method examples
        ("Site #1", 1),
        ("Site #2", 2),
        ("Site #32", 32),
    ),
)
def test_number_from_name(test_params: tuple[str, int]):
    name, number = test_params
    assert number_from_name(name) == number
