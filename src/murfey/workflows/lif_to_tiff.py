"""
Script to allow Murfey to submit the LIF-to-TIFF job to the cluster.
The recipe referred to here is stored on GitLab.
"""

from pathlib import Path

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set


def zocalo_cluster_request(
    file: Path, root_folder: str, messenger: TransportManager | None = None
):
    if messenger:
        # Set working directory to be the parent of the designated root folder
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
        working_dir = Path("/".join(new_path))

        messenger.send(
            "processing_recipe",
            {
                "recipes": ["lif-to-tiff"],
                "parameters": {
                    # Where the cluster generates and saves log files
                    "working_dir": str(working_dir),
                    "lif_path": str(file),
                    "root_dir": root_folder,
                },
            },
            new_connection=True,
        )
