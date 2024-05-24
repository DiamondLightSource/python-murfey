from pathlib import Path

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # If ISPyB credentials environment variable not set ignore


def zocalo_cluster_request(
    file: Path, root_folder: str, messenger: TransportManager | None = None
):
    if messenger:
        messenger.send(
            "processing_recipe",
            {
                "recipes": ["lif-to-tiff"],
                "parameters": {
                    "working_dir": str(file.parent.parent),
                    "lif_path": str(file),
                    "root_dir": root_folder,
                },
            },
            new_connection=True,
        )
