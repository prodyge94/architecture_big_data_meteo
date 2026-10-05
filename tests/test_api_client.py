import pytest

from meteo.api_client import ApiError, InfoclimatClient


class FakeResponse:
    def __init__(self, payload, status=200, headers=None):
        self._payload, self.status_code, self.headers, self.text = payload, status, headers or {}, ""

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls, self.headers = list(responses), [], {}

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params or {}))
        return self.responses.pop(0)


def test_pagination_suit_suite_from():
    session = FakeSession([
        FakeResponse({"tronque": True, "suite": {"from": "2000-01-03"}, "mesures": [{"date": "2000-01-01"}, {"date": "2000-01-02"}]}),
        FakeResponse({"tronque": False, "suite": None, "mesures": [{"date": "2000-01-03"}]}),
    ])
    client = InfoclimatClient("http://x", session=session)
    rows = client.journaliere("07156", "2000-01-01", "2000-02-01")
    assert [r["date"] for r in rows] == ["2000-01-01", "2000-01-02", "2000-01-03"]
    assert session.calls[1]["from"] == "2000-01-03"


def test_tronque_sans_suite_leve_une_erreur():
    session = FakeSession([FakeResponse({"tronque": True, "suite": None, "mesures": []})])
    with pytest.raises(ApiError):
        InfoclimatClient("http://x", session=session).journaliere("07156", "2000-01-01", "2000-02-01")


def test_retry_sur_503_avec_retry_after():
    session = FakeSession([
        FakeResponse({}, status=503, headers={"Retry-After": "0"}),
        FakeResponse({"tronque": False, "mesures": [{"date": "2000-01-01"}]}),
    ])
    rows = InfoclimatClient("http://x", session=session, backoff_s=0).journaliere("07156", "2000-01-01", "2000-01-02")
    assert len(rows) == 1 and len(session.calls) == 2
