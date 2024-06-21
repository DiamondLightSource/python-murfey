"""
Script to allow Murfey to submit the TIFF-to-stack job to the cluster.
The recipe referred to here is stored on GitLab.
"""

from pathlib import Path
from typing import Optional

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set


def zocalo_cluster_request(
    file: Path,
    root_folder: str,
    metadata: Optional[Path],
    messenger: TransportManager | None = None,
):
    if messenger:
        # Construct path to session directory
        path_parts = list(file.parts)
        new_path = []
        for p in range(len(path_parts)):
            part = path_parts[p]
            # Remove leading slash for subsequent rejoining
            if part == "/":
                part = ""
            # Append up to, but not including, root folder
            if part.lower() == root_folder.lower():
                break
            new_path.append(part)
        session_dir = Path("/".join(new_path))

        # If no metadata file provided, generate path to one
        if metadata is None:
            series_name = file.stem.split("--")[0]
            metadata = file.parent / "Metadata" / (series_name + ".xlif")

        messenger.send(
            "processing_recipe",
            {
                "recipes": ["tiff-to-stack"],
                "parameters": {
                    "session_dir": str(session_dir),
                    "tiff_path": str(file),
                    "root_dir": root_folder,
                    "metadata": str(metadata),
                },
            },
            new_connection=True,
        )
