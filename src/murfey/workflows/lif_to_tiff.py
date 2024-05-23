from pathlib import Path

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass


def zocalo_cluster_request(
    file: Path, root_folder: str, messenger: TransportManager | None = None
):
    if messenger:
        messenger.send(
            "processing_recipe",
            {
                "recipes": ["lif_to_tiff"],
                "parameters": {
                    "working_dir": str(file.parent.parent),
                    "lif_path": str(file),
                    "root_dir": root_folder,
                },
            },
            new_connection=True,
        )
