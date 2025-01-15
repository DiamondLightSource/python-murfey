def midpoint(angles: list[float]) -> int:
    """
    Utility function to calculate the midpoint of the angles used in a tilt series.
    Used primarily in the tomography workflow.
    """
    if not angles:
        return 0
    if len(angles) <= 2:
        return round(angles[0])
    sorted_angles = sorted(angles)
    return round(
        sorted_angles[len(sorted_angles) // 2]
        if sorted_angles[len(sorted_angles) // 2]
        and sorted_angles[len(sorted_angles) // 2 + 1]
        else 0
    )
