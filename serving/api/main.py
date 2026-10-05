"""API de serving des prévisions J+1 (lecture seule sur PostgreSQL). Doc interactive : /docs"""
import os
from datetime import date

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query

DSN = os.environ.get("METEO_PG_DSN", "postgresql://meteo:meteo@localhost:5432/meteo")

app = FastAPI(title="Prévision météo J+1", version="1.0",
              description="Prévisions Tmax/Tmin/pluie à J+1 par station (données Météo-France / Infoclimat)")


def query(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg2.connect(DSN) as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


@app.get("/health")
def health():
    query("SELECT 1")
    return {"status": "ok"}


@app.get("/stations")
def stations():
    return query("SELECT * FROM stations ORDER BY ic_id")


@app.get("/forecast/latest")
def latest(departement: str | None = None):
    """Dernière prévision disponible pour chaque station."""
    return query("""
        SELECT DISTINCT ON (p.ic_id) p.*, s.libelle, s.departement, s.latitude, s.longitude
        FROM predictions p JOIN stations s USING (ic_id)
        WHERE %s::text IS NULL OR s.departement = %s
        ORDER BY p.ic_id, p.date_cible DESC""", (departement, departement))


@app.get("/forecast/{ic_id}")
def forecast(ic_id: str, depuis: date | None = None, limit: int = Query(30, le=366)):
    """Historique prévu vs observé d'une station (les plus récents d'abord)."""
    rows = query("""
        SELECT p.ic_id, p.date_cible, p.date_emission, p.tmax_pred, o.tx AS tmax_obs,
               p.tmin_pred, o.tn AS tmin_obs, p.pluie_proba, o.rr AS rr_obs, p.modele_version, p.source
        FROM predictions p LEFT JOIN observations o ON o.ic_id = p.ic_id AND o.date = p.date_cible
        WHERE p.ic_id = %s AND (%s::date IS NULL OR p.date_cible >= %s)
        ORDER BY p.date_cible DESC LIMIT %s""", (ic_id, depuis, depuis, limit))
    if not rows:
        raise HTTPException(404, f"Aucune prévision pour la station {ic_id}")
    return rows


@app.get("/metrics")
def metrics(jours: int = Query(30, ge=1, le=366)):
    """Erreur des prévisions sur les N derniers jours observés (monitoring du modèle en production)."""
    return query("""
        SELECT count(*) AS n,
               avg(abs(p.tmax_pred - o.tx)) AS mae_tmax,
               avg(abs(p.tmin_pred - o.tn)) AS mae_tmin,
               avg(power(p.pluie_proba - (o.rr >= 1)::int, 2)) AS brier_pluie
        FROM predictions p JOIN observations o ON o.ic_id = p.ic_id AND o.date = p.date_cible
        WHERE p.date_cible > (SELECT max(date) FROM observations) - %s""", (jours,))[0]
