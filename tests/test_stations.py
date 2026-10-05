from meteo.stations import haversine_km, nearest_neighbors, select_stations


def feature(ic_id, **props):
    base = {"ic_id": ic_id, "mfid": "1", "genre": "synop", "pays": "FR", "ouverte": True, "departement": "75",
            "latitude": 48.8, "longitude": 2.3, "altitude": 50, "derniere_activite": "2026-09-01T00:00:00Z",
            "type_poste": "principale", "libelle": ic_id}
    base.update(props)
    return {"properties": base}


def test_selection_filtre_les_stations():
    ref = {"features": [
        feature("07156"),
        feature("07157", genre="static"),                         # pas synop
        feature("FMEE", departement="974"),                       # pas un indicatif OMM de métropole
        feature("07158", ouverte=False),                          # fermée
        feature("07159", derniere_activite="2025-01-01T00:00:00Z"),  # inactive
        feature("07160", departement="2A"),                       # Corse : dep[:2] non numérique, exclue
    ]}
    assert [s["ic_id"] for s in select_stations(ref, min_activity="2026-08-01")] == ["07156"]


def test_haversine_paris_lyon():
    assert 380 < haversine_km(48.8566, 2.3522, 45.764, 4.8357) < 400


def test_voisins_les_plus_proches():
    stations = [
        {"ic_id": "A", "latitude": 48.0, "longitude": 2.0},
        {"ic_id": "B", "latitude": 48.1, "longitude": 2.0},
        {"ic_id": "C", "latitude": 43.0, "longitude": 5.0},
    ]
    nn = nearest_neighbors(stations, k=1)
    assert {(n["ic_id"], n["voisin_id"]) for n in nn} == {("A", "B"), ("B", "A"), ("C", "A")}
