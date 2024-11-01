"""
Functions to process the requests received by Murfey related to the CLEM workflow.

The CLEM-related file registration API endpoints can eventually be moved here, since
the file registration processes all take place on the server side only.
"""


def register_lif_preprocessing_result(message: dict):
    """
    session_id (recipe)
    register (wrapper)
    output_files (wrapper)
        key1
        key2
        ...
    """
    pass


def register_tiff_preprocessing_result(message: dict):
    pass
