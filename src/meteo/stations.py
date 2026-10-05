"""Sélection des stations suivies et calcul des stations voisines."""
from __future__ import annotations

import math
import re

WMO_FR = re.compile(r"07\d{3}")  # indicatifs OMM des stations synoptiques de métropole


def select_stations(ref_geojson: dict, min_activity: str, max_stations: int = 0) -> list[dict]:
    """Stations synoptiques Météo-France de métropole, ouvertes et actives depuis `min_activity`.

    Ce sont les stations les plus fiables (mesures toutes les heures ou moins, historique long).
    """
    selected = []
    for feature in ref_geojson.get("features", []):
        p = feature["properties"]
        ic_id = p.get("ic_id") or ""
        dep = p.get("departement") or ""
        if not (p.get("genre") == "synop" and p.get("pays") == "FR" and p.get("mfid") and p.get("ouverte")):
            continue
        if not WMO_FR.fullmatch(ic_id) or not dep[:2].isdigit() or dep[:2] >= "96":
            continue
        if (p.get("derniere_activite") or "") < min_activity:
            continue
        selected.append({
            "ic_id": ic_id,
            "mfid": p["mfid"],
            "libelle": p.get("libelle"),
            "departement": dep,
            "latitude": p["latitude"],
            "longitude": p["longitude"],
            "altitude": p.get("altitude"),
            "type_poste": p.get("type_poste"),
        })
    selected.sort(key=lambda s: (s["type_poste"] != "principale", s["ic_id"]))
    if max_stations:
        selected = selected[:max_stations]
    return sorted(selected, key=lambda s: s["ic_id"])


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def nearest_neighbors(stations: list[dict], k: int) -> list[dict]:
    """Pour chaque station, ses k voisines les plus proches : [{ic_id, voisin_id, distance_km, rang}]."""
    out = []
    for s in stations:
        dists = sorted(
            (haversine_km(s["latitude"], s["longitude"], o["latitude"], o["longitude"]), o["ic_id"])
            for o in stations if o["ic_id"] != s["ic_id"]
        )
        for rank, (d, other) in enumerate(dists[:k], start=1):
            out.append({"ic_id": s["ic_id"], "voisin_id": other, "distance_km": round(d, 2), "rang": rank})
    return out
