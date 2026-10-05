"""Points d'entrée exécutables : `python -m meteo.jobs.<job>`."""
import logging


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
