from __future__ import annotations

import json
import logging
import time
import traceback
from ast import literal_eval
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, validator
from sqlmodel import Session

from murfey.util.db import CLEMImageSeries
from murfey.workflows.clem import get_db_entry

logger = logging.getLogger("murfey.workflows.clem.register_align_and_merge_results")


class AlignAndMergeResult(BaseModel):
    series_name: str
    image_stacks: list[Path]
    align_self: Optional[str] = None
    flatten: Optional[str] = "mean"
    align_across: Optional[str] = None
    composite_image: Path

    @validator(
        "image_stacks",
        pre=True,
    )
    def parse_stringified_list(cls, value):
        if isinstance(value, str):
            try:
                eval_result = literal_eval(value)
                if isinstance(eval_result, list):
                    parent_tiffs = [Path(p) for p in eval_result]
                    return parent_tiffs
            except (SyntaxError, ValueError):
                raise ValueError("Unable to parse input")
        # Return value as-is; if it fails, it fails
        return value


def register_align_and_merge_result(
    message: dict, murfey_db: Session, demo: bool = False
) -> bool:
    """
    session_id (recipe)
    register (wrapper)
    result (wrapper)
        key1
        key2
        ...
    """

    session_id: int = (
        int(message["session_id"])
        if not isinstance(message["session_id"], int)
        else message["session_id"]
    )

    # Validate message and try and load results
    try:
        if isinstance(message["result"], str):
            json_obj: dict = json.loads(message["result"])
            result = AlignAndMergeResult(**json_obj)
        elif isinstance(message["result"], dict):
            result = AlignAndMergeResult(**message["result"])
        else:
            logger.error(
                "Invalid type for align-and-merge processing result: "
                f"{type(message['result'])}"
            )
            return False
    except Exception:
        logger.error(
            "Exception encountered when parsing align-and-merge processing result: \n"
            f"{traceback.format_exc()}"
        )
        return False

    # Outer try-finally block for tidying up database-related section of function
    try:
        # Register items in database if not already present
        try:
            clem_img_series: CLEMImageSeries = get_db_entry(
                db=murfey_db,
                table=CLEMImageSeries,
                session_id=session_id,
                series_name=result.series_name,
            )
            clem_img_series.composite_image = str(result.composite_image)
            clem_img_series.composite_created = True
            murfey_db.add(clem_img_series)
            murfey_db.commit()

            # Make multiple attempts to refresh data in case of race condition
            attempts = 0
            while attempts < 50:
                try:
                    murfey_db.refresh(clem_img_series)
                    break
                except Exception:
                    logger.warning(
                        f"Attempt {attempts + 1} at refreshing database entry for "
                        f"{str(result.series_name)!r} failed: \n"
                        f"{traceback.format_exc()}"
                    )
                    attempts += 1
                    time.sleep(0.1)
            else:
                raise RuntimeError(
                    "Maximum number of attempts reached while trying to refresh database "
                    f"entry for {result.series_name!r}"
                )

            logger.info(
                "Align-and-merge processing result registered for "
                f"{result.series_name!r} series"
            )

        except Exception:
            logger.error(
                "Exception encountered when registering LIF preprocessing result for "
                f"{result.series_name!r} {result.channel!r} image stack: \n"
                f"{traceback.format_exc()}"
            )
            return False

        return True
    finally:
        murfey_db.close()
