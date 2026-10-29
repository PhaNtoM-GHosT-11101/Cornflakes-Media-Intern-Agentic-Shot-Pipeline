import httpx
import pytest

from helpers import BASE, ensure_balance

pytest.importorskip("httpx")


@pytest.fixture()
def api():
    try:
        client = httpx.Client(base_url=BASE, timeout=30.0)
        client.get("/healthz").raise_for_status()
        ensure_balance(client)
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"pipeline at {BASE} is not reachable ({exc}). "
            "Start it first: `make local` or `docker compose up --build`."
        ) from exc
    yield client
    client.close()
