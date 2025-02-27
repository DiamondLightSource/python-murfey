from typing import List, Optional, Tuple

from sqlmodel import Session, select

from murfey.util.db import DataCollectionGroup, GridSquare


def check_for_common_grid_squares(
    murfey_db: Session,
    tag: str,
    session_id: int,
    grid_square_names: Optional[List[int]] = None,
) -> Tuple[Optional[int], str]:
    data_collection_groups = murfey_db.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.session_id == session_id)
    ).all()
    grid_squares_matching_tag = [
        gs.name
        for gs in murfey_db.exec(
            select(GridSquare)
            .where(GridSquare.tag == tag)
            .where(GridSquare.session_id == session_id)
        ).all()
    ]
    if grid_square_names is not None:
        grid_squares_matching_tag.extend(grid_square_names)
    for dcg in data_collection_groups:
        grid_squares = murfey_db.exec(
            select(GridSquare).where(GridSquare.tag == dcg.tag)
        ).all()
        if set(grid_squares_matching_tag).intersection(
            {gs.name for gs in grid_squares}
        ):
            return (dcg.id, dcg.tag)
    return (None, "")


def replace_grid_square_tags(murfey_db: Session, old_tag: str, new_tag: str):
    grid_squares = murfey_db.exec(
        select(GridSquare).where(GridSquare.tag == old_tag)
    ).all()
    for gs in grid_squares:
        gs.tag = new_tag
    murfey_db.add(grid_squares)
    murfey_db.commit()
    murfey_db.close()
