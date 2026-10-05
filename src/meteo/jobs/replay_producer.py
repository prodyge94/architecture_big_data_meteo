"""Producteur Kafka : publie les observations journalières comme si elles arrivaient en temps réel.

  --mode replay : rejoue l'historique stocké en bronze, jour par jour, à vitesse accélérée
                  (ex. 1 jour simulé toutes les 5 s). C'est ce qui permet de tester le streaming de bout en
                  bout, la source n'étant publiée qu'une fois par jour.
  --mode live   : interroge l'API pour les derniers jours et publie (à lancer quotidiennement).
"""
import argparse
import datetime as dt
import json
import logging
import time
from collections import defaultdict

from confluent_kafka import Producer

from meteo.api_client import InfoclimatClient
from meteo.config import SELECTION_KEY, settings
from meteo.jobs import setup_logging
from meteo.lake import Lake

log = logging.getLogger("producer")


def load_from_bronze(lake: Lake, start: str, end: str) -> dict[str, list[dict]]:
    by_day: dict[str, dict[str, dict]] = defaultdict(dict)
    prefix = f"bronze/infoclimat/{settings.api_famille}_journaliere/"
    for key in sorted(lake.list_keys(prefix)):  # tri : les ingestions récentes écrasent les anciennes
        for row in lake.get_json(key)["mesures"]:
            if start <= row["date"] < end:
                by_day[row["date"]][row["ic_id"]] = row
    return {day: list(rows.values()) for day, rows in sorted(by_day.items())}


def load_from_api(lake: Lake, days: int) -> dict[str, list[dict]]:
    client = InfoclimatClient(settings.api_base_url)
    today = dt.date.today()
    start, end = (today - dt.timedelta(days=days)).isoformat(), (today + dt.timedelta(days=1)).isoformat()
    by_day: dict[str, list[dict]] = defaultdict(list)
    for s in lake.get_json(SELECTION_KEY)["stations"]:
        for row in client.journaliere(s["ic_id"], start, end, famille=settings.api_famille):
            by_day[row["date"]].append(row)
    return dict(sorted(by_day.items()))


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["replay", "live"], default="replay")
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default="2026-01-01", help="exclu")
    p.add_argument("--secondes-par-jour", type=float, default=5.0)
    p.add_argument("--jours", type=int, default=3, help="mode live : nombre de jours récupérés")
    args = p.parse_args()

    lake = Lake(settings)
    days = load_from_bronze(lake, args.start, args.end) if args.mode == "replay" else load_from_api(lake, args.jours)
    log.info("%d jours à publier sur %s (%s)", len(days), settings.kafka_topic, args.mode)

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "linger.ms": 50,
                         "compression.type": "lz4", "acks": "all"})
    for day, rows in days.items():
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        for row in rows:
            producer.produce(settings.kafka_topic, key=row["ic_id"],
                             value=json.dumps({**row, "ingested_at": now}).encode("utf-8"))
        producer.flush()
        log.info("%s : %d observations publiées", day, len(rows))
        if args.mode == "replay":
            time.sleep(args.secondes_par_jour)


if __name__ == "__main__":
    main()
