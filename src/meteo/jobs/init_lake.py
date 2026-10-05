"""Crée les buckets du data lake et le schéma PostgreSQL de serving."""
import logging

from meteo import db
from meteo.config import settings
from meteo.jobs import setup_logging
from meteo.lake import Lake

log = logging.getLogger("init_lake")


def main() -> None:
    setup_logging()
    Lake(settings).ensure_buckets()
    db.ensure_schema(settings.pg_dsn)
    log.info("Buckets et schéma de serving prêts")


if __name__ == "__main__":
    main()
