"""Tests des transformations Spark en local (sans lake)."""
import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from meteo.transform import (FEATURES, OBS_SCHEMA, build_features, clean_daily, climatology,  # noqa: E402
                             dedup_latest)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def raw_row(ic_id, day, tn, tx, rr=0.0, statut="quotidien", ingested="2026-10-01T00:00:00Z"):
    return {"ic_id": ic_id, "mfid": "1", "date": day, "tn": tn, "tx": tx, "tm": None, "rr": rr,
            "n_obs": 24, "n_obs_rejetees": 0, "statut_extremes": statut, "ingested_at": ingested}


def test_clean_daily_qc(spark):
    df = spark.createDataFrame([
        raw_row("A", "2024-01-01", 1.0, 5.0),
        raw_row("A", "2024-01-02", 1.0, 5.0, statut="partiel"),    # extrêmes non fiables -> tn/tx null
        raw_row("A", "2024-01-03", 8.0, 2.0),                       # tx < tn -> rejeté
        raw_row("A", "2024-01-04", 1.0, 5.0, rr=-3.0),              # pluie impossible -> null
    ], OBS_SCHEMA)
    out = {r.date.isoformat(): r for r in clean_daily(df).collect()}
    assert out["2024-01-01"].tx == 5.0 and out["2024-01-01"].tm == 3.0
    assert out["2024-01-02"].tx is None and out["2024-01-02"].rr == 0.0
    assert out["2024-01-03"].tx is None
    assert out["2024-01-04"].rr is None


def test_dedup_garde_la_derniere_ingestion(spark):
    df = clean_daily(spark.createDataFrame([
        raw_row("A", "2024-01-01", 1.0, 5.0, ingested="2026-01-01T00:00:00Z"),
        raw_row("A", "2024-01-01", 1.0, 6.0, ingested="2026-02-01T00:00:00Z"),
    ], OBS_SCHEMA))
    rows = dedup_latest(df).collect()
    assert len(rows) == 1 and rows[0].tx == 6.0


def test_build_features_sans_fuite(spark):
    start = dt.date(2024, 1, 1)
    rows = []
    for i in range(20):
        day = (start + dt.timedelta(days=i)).isoformat()
        rows.append(raw_row("A", day, float(i), float(i) + 10, rr=2.0 if i % 2 else 0.0))
        rows.append(raw_row("B", day, float(i) + 1, float(i) + 11))
    obs = clean_daily(spark.createDataFrame(rows, OBS_SCHEMA))
    stations = spark.createDataFrame([("A", 48.0, 2.0, 50), ("B", 48.1, 2.0, 60)],
                                     ["ic_id", "latitude", "longitude", "altitude"])
    voisins = spark.createDataFrame([("A", "B"), ("B", "A")], ["ic_id", "voisin_id"])
    clim = climatology(obs, "2030-01-01", 1.0)

    feats = build_features(obs, stations, voisins, clim, 1.0)
    assert set(FEATURES) <= set(feats.columns)
    r = feats.filter("ic_id = 'A' AND date = '2024-01-10'").first()
    assert r.tx == 19.0 and r.tx_lag1 == 18.0 and r.tx_lag3 == 16.0
    assert r.tx_j1 == 20.0                      # cible = lendemain
    assert r.vois_tx == 20.0                    # voisine B le même jour
    assert r.pluie_j1 == 0.0                    # 2024-01-11 : i=10 pair -> pas de pluie
    assert r.tx_moy7 == pytest.approx(16.0)     # moyenne de 13..19
    last = feats.filter("ic_id = 'A' AND date = '2024-01-20'").first()
    assert last.tx_j1 is None                   # pas de lendemain connu
