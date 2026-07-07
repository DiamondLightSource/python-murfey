from __future__ import annotations

import json
import logging
import traceback
from ast import literal_eval
from functools import cached_property
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, computed_field, field_validator
from sqlmodel import Session, select

from murfey.util.db import ImagingSite

logger = logging.getLogger("murfey.workflows.clem.register_align_and_merge_results")


class AlignAndMergeResult(BaseModel):
    series_name: str
    image_stacks: list[Path]
    align_self: bool = False
    flatten: bool = True
    align_across: bool = False
    output_file: Path
    thumbnail: Optional[Path] = None
    thumbnail_size: Optional[tuple[int, int]] = None

    # Valid Pydantic decorator not supported by MyPy
    @computed_field  # type: ignore
    @cached_property
    def is_denoised(self) -> bool:
        """
        The "_Lng_LVCC" suffix appended to a CLEM dataset's position name indicates
        that it's a denoised image set of the same position. These results should
        override or supersede the original ones once they're available.
        """
        return "_Lng_LVCC" in self.series_name

    # Valid Pydantic decorator not supported by MyPy
    @computed_field  # type: ignore
    @cached_property
    def site_name(self) -> str:
        """
        Extract just the name of the site by removing the "_Lng_LVCC" suffix from
        the series name.
        """
        return self.series_name.replace("_Lng_LVCC", "")

    @field_validator("image_stacks", mode="before")
    @classmethod
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


def run(message: dict, murfey_db: Session) -> dict[str, bool]:
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
            return {"success": False, "requeue": False}
    except Exception:
        logger.error(
            "Exception encountered when parsing align-and-merge processing result: \n"
            f"{traceback.format_exc()}"
        )
        return {"success": False, "requeue": False}

    # Outer try-finally block for tidying up database-related section of function
    try:
        try:
            clem_img_site = murfey_db.exec(
                select(ImagingSite)
                .where(ImagingSite.session_id == session_id)
                .where(ImagingSite.site_name == result.site_name)
            ).one()

            # Update the stored entry only if the incoming one matches it
            if clem_img_site.image_path is not None and (
                # Denoised dataset results should be registered regardless
                result.is_denoised
                or (
                    # Raw dataset result should only be considered
                    # If the current entry is also a raw dataset
                    not result.is_denoised
                    and "_Lng_LVCC" not in clem_img_site.image_path
                )
            ):
                clem_img_site.composite_created = True
                murfey_db.add(clem_img_site)
                murfey_db.commit()

                logger.info(
                    "Align-and-merge processing result registered for "
                    f"{result.series_name!r} series"
                )
            else:
                logger.info(
                    "Skipping database registration as incoming result doesn't match stored entry"
                )

        except Exception:
            logger.error(
                "Exception encountered when registering align-and-merge result for "
                f"{result.series_name!r}: \n"
                f"{traceback.format_exc()}"
            )
            return {"success": False, "requeue": False}

        return {"success": True}
    finally:
        murfey_db.close()
