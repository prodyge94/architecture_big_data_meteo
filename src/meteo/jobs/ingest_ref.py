"""Ingestion du référentiel des stations (bronze) + sélection des stations suivies."""
import datetime as dt
import logging

from meteo.api_client import InfoclimatClient
from meteo.config import SELECTION_KEY, settings
from meteo.jobs import setup_logging
from meteo.lake import Lake
from meteo.stations import select_stations

log = logging.getLogger("ingest_ref")


def main() -> None:
    setup_logging()
    client = InfoclimatClient(settings.api_base_url)
    lake = Lake(settings)
    now = dt.datetime.now(dt.timezone.utc)

    ref = client.stations_ref()
    key = f"bronze/infoclimat/ref_stations/ingest_date={now:%Y-%m-%d}/stations.geojson"
    lake.put_json(key, ref)
    log.info("Référentiel : %d stations -> s3://%s/%s", len(ref.get("features", [])), settings.lake_bucket, key)

    # Le champ derniere_activite du référentiel peut avoir des semaines de retard sur les séries :
    # on mesure l'activité par rapport à la station la plus récente du référentiel, pas à aujourd'hui.
    newest = max(f["properties"].get("derniere_activite") or "" for f in ref["features"])[:10]
    min_activity = (dt.date.fromisoformat(newest) - dt.timedelta(days=30)).isoformat()
    stations = select_stations(ref, min_activity=min_activity, max_stations=settings.max_stations)
    if not stations:
        raise SystemExit(f"Aucune station sélectionnée (activité >= {min_activity})")
    lake.put_json(SELECTION_KEY, {"generated_at": now.isoformat(), "critere":
                                  "synop Météo-France métropole, ouverte, active depuis " + min_activity,
                                  "stations": stations})
    log.info("%d stations sélectionnées -> s3://%s/%s", len(stations), settings.lake_bucket, SELECTION_KEY)


if __name__ == "__main__":
    main()
