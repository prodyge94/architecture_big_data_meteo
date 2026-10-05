"""Ingestion des séries journalières dans la couche bronze (JSON brut, une réponse par station).

  python -m meteo.jobs.ingest_daily              # incrémental : 10 derniers jours
  python -m meteo.jobs.ingest_daily --full       # backfill depuis HISTORY_START
  python -m meteo.jobs.ingest_daily --start 2024-01-01 --end 2025-01-01
"""
import argparse
import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from meteo.api_client import InfoclimatClient
from meteo.config import SELECTION_KEY, settings
from meteo.jobs import setup_logging
from meteo.lake import Lake

log = logging.getLogger("ingest_daily")


def parse_args():
    today = dt.date.today()
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=(today - dt.timedelta(days=10)).isoformat())
    p.add_argument("--end", default=(today + dt.timedelta(days=1)).isoformat(), help="exclu")
    p.add_argument("--full", action="store_true", help="tout l'historique depuis HISTORY_START")
    return p.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    start = settings.history_start if args.full else args.start
    lake = Lake(settings)
    client = InfoclimatClient(settings.api_base_url)
    stations = [s["ic_id"] for s in lake.get_json(SELECTION_KEY)["stations"]]
    now = dt.datetime.now(dt.timezone.utc)
    prefix = f"bronze/infoclimat/{settings.api_famille}_journaliere/ingest_date={now:%Y-%m-%d}"
    log.info("Ingestion %s -> %s pour %d stations (%d en parallèle)", start, args.end, len(stations),
             settings.api_workers)

    def fetch(station: str) -> int:
        rows = client.journaliere(station, start, args.end, famille=settings.api_famille)
        lake.put_json(f"{prefix}/station={station}/{start}_{args.end}.json", {
            "station": station,
            "requete": {"famille": settings.api_famille, "from": start, "to": args.end},
            "ingested_at": now.isoformat(),
            "source": settings.api_base_url,
            "n": len(rows),
            "mesures": rows,
        })
        return len(rows)

    total, failed = 0, []
    with ThreadPoolExecutor(max_workers=settings.api_workers) as pool:
        futures = {pool.submit(fetch, s): s for s in stations}
        for i, fut in enumerate(as_completed(futures), start=1):
            station = futures[fut]
            try:
                total += fut.result()
            except Exception as exc:
                failed.append(station)
                log.error("Station %s en échec : %s", station, exc)
            if i % 20 == 0 or i == len(stations):
                log.info("%d/%d stations, %d lignes", i, len(stations), total)

    log.info("Terminé : %d lignes, %d stations en échec %s", total, len(failed), failed)
    if len(failed) > 0.1 * len(stations):
        raise SystemExit("Plus de 10 % des stations en échec")


if __name__ == "__main__":
    main()
