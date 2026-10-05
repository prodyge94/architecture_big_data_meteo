"""Client de l'API REST climatologique Infoclimat (https://portail.chom.engineering/).

Points d'attention de l'API (cf. /llms-full.txt) :
- 20 000 lignes max par requête : il faut paginer avec `suite.from` et toujours tester `tronque` ;
- pas de quota, mais l'API tourne sur une seule machine : peu de parallélisme, retry + backoff ;
- pendant la chauffe, 503 + `Retry-After` qu'il faut respecter.
"""
from __future__ import annotations

import logging
import time
from typing import Iterator

import requests

log = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}


class ApiError(RuntimeError):
    pass


class InfoclimatClient:
    def __init__(self, base_url: str, timeout: float = 120, max_retries: int = 5,
                 backoff_s: float = 2.0, session: requests.Session | None = None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_s = backoff_s
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "esgi-archi-big-data-meteo/1.0")

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                wait = self.backoff_s * 2 ** (attempt - 1)
                log.warning("GET %s %s -> %s (tentative %d, attente %.0fs)", url, params, exc, attempt, wait)
                time.sleep(wait)
                continue
            if resp.status_code in RETRY_STATUS:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else self.backoff_s * 2 ** (attempt - 1)
                log.warning("GET %s %s -> HTTP %d (tentative %d, attente %.0fs)",
                            url, params, resp.status_code, attempt, wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                raise ApiError(f"GET {url} {params} -> HTTP {resp.status_code}: {resp.text[:300]}")
            return resp.json()
        raise ApiError(f"GET {url} {params} : échec après {self.max_retries} tentatives")

    def iter_pages(self, path: str, params: dict) -> Iterator[dict]:
        """Itère sur les pages d'une série en suivant `suite.from`."""
        params = dict(params)
        while True:
            page = self.get(path, params)
            yield page
            if not page.get("tronque"):
                return
            suite = page.get("suite")
            if not suite:
                raise ApiError(f"{path} {params} : réponse tronquée sans point de reprise, resserrer la requête")
            params["from"] = suite["from"]

    def journaliere(self, station: str, start: str, end: str, famille: str = "v2") -> list[dict]:
        """Série journalière d'une station sur [start, end), toutes pages concaténées."""
        rows: list[dict] = []
        for page in self.iter_pages(f"/{famille}/journaliere", {"station": station, "from": start, "to": end}):
            rows.extend(page.get("mesures", []))
        return rows

    def stations_ref(self) -> dict:
        """Référentiel des stations (GeoJSON FeatureCollection, non paginé)."""
        return self.get("/ref/stations")
