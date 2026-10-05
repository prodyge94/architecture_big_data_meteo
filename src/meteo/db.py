"""Couche de serving : PostgreSQL (dernières prévisions, observations récentes, stations)."""
from __future__ import annotations

from typing import Iterable

import psycopg2
from psycopg2.extras import execute_values

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    ic_id        TEXT PRIMARY KEY,
    mfid         TEXT,
    libelle      TEXT,
    departement  TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    altitude     DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS observations (
    ic_id  TEXT NOT NULL,
    date   DATE NOT NULL,
    tn     DOUBLE PRECISION,
    tx     DOUBLE PRECISION,
    rr     DOUBLE PRECISION,
    PRIMARY KEY (ic_id, date)
);
CREATE TABLE IF NOT EXISTS predictions (
    ic_id           TEXT NOT NULL,
    date_cible      DATE NOT NULL,
    date_emission   DATE NOT NULL,
    tmax_pred       DOUBLE PRECISION,
    tmin_pred       DOUBLE PRECISION,
    pluie_proba     DOUBLE PRECISION,
    modele_version  TEXT,
    source          TEXT,
    cree_le         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ic_id, date_cible)
);
CREATE INDEX IF NOT EXISTS predictions_date_cible ON predictions (date_cible);
"""


def connect(dsn: str):
    return psycopg2.connect(dsn)


def ensure_schema(dsn: str) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA)


def upsert_stations(dsn: str, rows: Iterable[tuple]) -> None:
    sql = """INSERT INTO stations (ic_id, mfid, libelle, departement, latitude, longitude, altitude)
             VALUES %s ON CONFLICT (ic_id) DO UPDATE SET mfid = EXCLUDED.mfid, libelle = EXCLUDED.libelle,
             departement = EXCLUDED.departement, latitude = EXCLUDED.latitude,
             longitude = EXCLUDED.longitude, altitude = EXCLUDED.altitude"""
    with connect(dsn) as conn, conn.cursor() as cur:
        execute_values(cur, sql, list(rows))


def upsert_observations(dsn: str, rows: Iterable[tuple]) -> None:
    sql = """INSERT INTO observations (ic_id, date, tn, tx, rr) VALUES %s
             ON CONFLICT (ic_id, date) DO UPDATE SET tn = EXCLUDED.tn, tx = EXCLUDED.tx, rr = EXCLUDED.rr"""
    rows = list(rows)
    if rows:
        with connect(dsn) as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)


def upsert_predictions(dsn: str, rows: Iterable[tuple]) -> None:
    sql = """INSERT INTO predictions (ic_id, date_cible, date_emission, tmax_pred, tmin_pred, pluie_proba,
                                      modele_version, source) VALUES %s
             ON CONFLICT (ic_id, date_cible) DO UPDATE SET date_emission = EXCLUDED.date_emission,
             tmax_pred = EXCLUDED.tmax_pred, tmin_pred = EXCLUDED.tmin_pred, pluie_proba = EXCLUDED.pluie_proba,
             modele_version = EXCLUDED.modele_version, source = EXCLUDED.source, cree_le = now()"""
    rows = list(rows)
    if rows:
        with connect(dsn) as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
