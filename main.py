"""Main class contain CLI commands."""

import fire

from build_model import build_models
from extract_and_load import ingest_gdrive
from extract_and_load import clean


class MainCommands:
    """Contains all commans to be run as CLI or docker run."""

    def __init__(self) -> None:
        """Init."""
        self.ingest_gdrive = ingest_gdrive
        self.clean = clean
        self.build_models = build_models


if __name__ == "__main__":
    fire.Fire(MainCommands())