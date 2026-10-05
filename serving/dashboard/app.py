"""Tableau de bord : prévisions J+1, comparaison prévu / observé, erreur du modèle."""
import os

import pandas as pd
import psycopg2
import streamlit as st

DSN = os.environ.get("METEO_PG_DSN", "postgresql://meteo:meteo@localhost:5432/meteo")

st.set_page_config(page_title="Prévision météo J+1", layout="wide")


@st.cache_data(ttl=30)
def load(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg2.connect(DSN) as conn:
        return pd.read_sql_query(sql, conn, params=params)


st.title("Prévision météo à J+1")
st.caption("Données Météo-France / Infoclimat · modèles Spark ML (GBT) · pipeline bronze → silver → gold")

try:
    latest = load("""
        SELECT DISTINCT ON (p.ic_id) p.ic_id, s.libelle, s.departement, s.latitude AS lat, s.longitude AS lon,
               p.date_cible, p.tmax_pred, p.tmin_pred, p.pluie_proba, p.source, p.modele_version
        FROM predictions p JOIN stations s USING (ic_id) ORDER BY p.ic_id, p.date_cible DESC""")
except Exception as exc:  # base vide au premier démarrage
    st.info(f"Pas encore de prévisions : lancer le DAG `meteo_backfill` dans Airflow. ({exc})")
    st.stop()

if latest.empty:
    st.info("Pas encore de prévisions : lancer le DAG `meteo_backfill` dans Airflow.")
    st.stop()

err = load("""
    SELECT p.date_cible, avg(abs(p.tmax_pred - o.tx)) AS mae_tmax_modele,
           avg(abs(p.tmin_pred - o.tn)) AS mae_tmin_modele, count(*) AS n
    FROM predictions p JOIN observations o ON o.ic_id = p.ic_id AND o.date = p.date_cible
    GROUP BY 1 ORDER BY 1""")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Stations", len(latest))
c2.metric("Dernière échéance", str(latest["date_cible"].max()))
if not err.empty:
    recent = err.tail(30)
    c3.metric("MAE Tmax (30 j)", f"{(recent.mae_tmax_modele * recent.n).sum() / recent.n.sum():.2f} °C")
    c4.metric("MAE Tmin (30 j)", f"{(recent.mae_tmin_modele * recent.n).sum() / recent.n.sum():.2f} °C")

st.subheader("Dernières prévisions")
left, right = st.columns([3, 2])
with left:
    st.map(latest, latitude="lat", longitude="lon", size=4000)
with right:
    st.dataframe(latest[["ic_id", "libelle", "date_cible", "tmax_pred", "tmin_pred", "pluie_proba"]]
                 .sort_values("libelle"), hide_index=True, height=420)

st.subheader("Prévu vs observé par station")
labels = (latest["libelle"].fillna(latest["ic_id"]) + " (" + latest["ic_id"] + ")").tolist()
choice = st.selectbox("Station", sorted(labels))
ic_id = choice.rsplit("(", 1)[1].rstrip(")")
serie = load("""
    SELECT p.date_cible, p.tmax_pred, o.tx AS tmax_obs, p.tmin_pred, o.tn AS tmin_obs,
           p.pluie_proba, o.rr AS rr_obs
    FROM predictions p LEFT JOIN observations o ON o.ic_id = p.ic_id AND o.date = p.date_cible
    WHERE p.ic_id = %s ORDER BY p.date_cible""", (ic_id,)).set_index("date_cible")
st.line_chart(serie[["tmax_pred", "tmax_obs", "tmin_pred", "tmin_obs"]],
              color=["#d62728", "#ff9896", "#1f77b4", "#aec7e8"])
st.bar_chart(serie[["pluie_proba"]])

if not err.empty:
    st.subheader("Erreur absolue moyenne par jour (toutes stations)")
    st.line_chart(err.set_index("date_cible")[["mae_tmax_modele", "mae_tmin_modele"]])
