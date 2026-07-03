"""
Tests for the CCP-EM Pipeliner job-number reservation helpers in murfey.server.feedback

These guard the duplicate-job-number fix: SPA feedback used to read the
Pipeliner job counter without advancing it, so two jobs scheduled before the
first had been registered by the node creator reused the same number. The
helpers below now reserve (advance) the counter under the project lock at
schedule time.
"""

from types import SimpleNamespace
from unittest import mock

import pytest


@pytest.fixture
def feedback():
    """Import murfey.server.feedback with its module-level DB setup stubbed out."""
    with (
        mock.patch(
            "murfey.util.config.get_security_config", return_value=mock.MagicMock()
        ),
        mock.patch("murfey.server.murfey_db.url", return_value=mock.MagicMock()),
        mock.patch("sqlmodel.create_engine", return_value=mock.MagicMock()),
    ):
        import murfey.server.feedback as feedback_module

        yield feedback_module


def _make_pipeline(pipeline_dir, job_counter: int) -> None:
    """Write a valid default_pipeline.star carrying the given job counter."""
    from pipeliner.project_graph import ProjectGraph

    with ProjectGraph(
        create_new=True,
        read_only=False,
        pipeline_dir=str(pipeline_dir),
        name="default",
    ) as project:
        project.job_counter = job_counter


def _read_counter(pipeline_dir) -> int:
    from pipeliner.project_graph import ProjectGraph

    with ProjectGraph(
        read_only=True, pipeline_dir=str(pipeline_dir), name="default"
    ) as project:
        return project.job_counter


def test_reserve_advances_counter_and_returns_base(feedback, tmp_path):
    _make_pipeline(tmp_path, job_counter=12)

    base = feedback._reserve_pipeline_job_numbers(str(tmp_path), 3)

    assert base == 12
    # The counter is consumed now, not when the job later registers.
    assert _read_counter(tmp_path) == 15


def test_reserve_blocks_are_contiguous_and_non_overlapping(feedback, tmp_path):
    _make_pipeline(tmp_path, job_counter=12)

    first = feedback._reserve_pipeline_job_numbers(str(tmp_path), 2)
    second = feedback._reserve_pipeline_job_numbers(str(tmp_path), 2)

    assert first == 12
    assert second == 14  # strictly after the first block — never reused
    assert _read_counter(tmp_path) == 16


def test_reserve_floors_at_first_feedback_job(feedback, tmp_path):
    # Counter still in the preprocessing range (Extract=5/Select=6 not yet
    # registered). A Class2D job must never be handed 5 or 6.
    _make_pipeline(tmp_path, job_counter=5)

    base = feedback._reserve_pipeline_job_numbers(str(tmp_path), 2)

    assert base == feedback.FIRST_FEEDBACK_JOB == 7
    assert _read_counter(tmp_path) == 9


def test_reserve_missing_pipeline_falls_back_without_creating(feedback, tmp_path):
    base = feedback._reserve_pipeline_job_numbers(str(tmp_path), 2)

    assert base == feedback.FIRST_FEEDBACK_JOB
    assert not (tmp_path / "default_pipeline.star").exists()


def test_reserve_rejects_non_positive_block(feedback, tmp_path):
    _make_pipeline(tmp_path, job_counter=10)

    with pytest.raises(ValueError):
        feedback._reserve_pipeline_job_numbers(str(tmp_path), 0)


@pytest.mark.parametrize(
    "icebreaker, first_block, combine_offset, subsequent_block",
    [
        # no icebreaker: Class2D + autoselect + (combine on first batch only)
        (False, 3, 2, 2),
        # icebreaker: Class2D + IceBreaker + autoselect + (combine on first batch)
        (True, 4, 3, 3),
    ],
)
def test_reserve_2d_classification_block(
    feedback,
    tmp_path,
    icebreaker,
    first_block,
    combine_offset,
    subsequent_block,
):
    _make_pipeline(tmp_path, job_counter=10)
    fp = SimpleNamespace(star_combination_job=0, next_job=0)

    with mock.patch.object(
        feedback.default_spa_parameters, "do_icebreaker_jobs", icebreaker
    ):
        # First batch reserves Class2D (+IceBreaker) + autoselect + shared combine.
        base1 = feedback._reserve_2d_classification_jobs(str(tmp_path), fp)
        assert base1 == 10
        assert fp.next_job == 10
        assert fp.star_combination_job == 10 + combine_offset
        assert _read_counter(tmp_path) == 10 + first_block

        # select_classes places autoselect at class2d + (2 if icebreaker else 1),
        # which must equal combine - 1 so the layout stays contiguous.
        autoselect = base1 + (2 if icebreaker else 1)
        assert autoselect == fp.star_combination_job - 1

        # Second batch: combine already exists (shared) → only Class2D + autoselect.
        combine_before = fp.star_combination_job
        base2 = feedback._reserve_2d_classification_jobs(str(tmp_path), fp)
        assert base2 == 10 + first_block  # strictly after the first block
        assert fp.star_combination_job == combine_before  # combine unchanged
        assert _read_counter(tmp_path) == base2 + subsequent_block
