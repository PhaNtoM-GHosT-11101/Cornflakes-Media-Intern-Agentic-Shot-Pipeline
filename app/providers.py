"""Provider layer: timeouts, retries with backoff, 402 handling and fallback.

The demo has three video providers (A cheap/slow, B mid, C premium) and one
image provider. All are mocks; no real API is called.
"""

import asyncio
import random

import httpx

from . import cache, config

TIMEOUT = httpx.Timeout(10.0, connect=3.0)
_clients: dict[str, httpx.AsyncClient] = {}


def client() -> httpx.AsyncClient:
    c = _clients.get("c")
    if c is None:
        c = httpx.AsyncClient(base_url=config.MOCKS_URL, timeout=TIMEOUT)
        _clients["c"] = c
    return c


class ProviderUnavailable(Exception):
    def __init__(self, provider: str, reason: str):
        self.provider = provider
        self.reason = reason


async def _post(path: str, payload: dict) -> dict:
    r = await client().post(path, json=payload)
    if r.status_code == 200:
        return r.json()
    if r.status_code == 402:
        raise ProviderUnavailable(payload.get("provider", "?"), "out of funds (402)")
    raise httpx.HTTPStatusError(f"{r.status_code} from {path}", request=r.request, response=r)


async def _with_retries(fn, attempts: int = 2, base: float = 0.3):
    last: Exception | None = None
    for i in range(attempts):
        try:
            return await fn()
        except ProviderUnavailable:
            raise
        except Exception as exc:  # 5xx / timeout / 429-after-mock
            last = exc
            if i + 1 < attempts:
                await asyncio.sleep(base * (2 ** i) + random.uniform(0, 0.15))
    raise last  # type: ignore[misc]


async def plan(story: str, shots: int) -> dict:
    return await _with_retries(lambda: _post("/plan", {"story": story, "shots": shots}))


async def image(prompt: str, seed: int, reference: str) -> dict:
    return await _with_retries(
        lambda: _post("/image", {"prompt": prompt, "seed": seed, "reference": reference})
    )


async def check(reference: str, shots: list[dict]) -> dict:
    return await _post("/check", {"reference": reference, "shots": shots})


async def compose(job_id: str, shots: list[dict]) -> dict:
    return await _post("/compose", {"job_id": job_id, "shots": shots})


async def video_routed(prompt: str, seed: int, reference: str) -> dict:
    """Try providers cheapest-first. 402 removes a provider, other errors fall back.

    Returns {"result": ..., "provider": name, "degraded": bool, "down": [names]}
    """
    order = ["A", "B", "C"]
    down: list[str] = []
    last_error = "no provider available"

    for idx, name in enumerate(order):
        if not await cache.provider_available(name):
            continue
        payload = {"prompt": prompt, "seed": seed, "reference": reference,
                   "provider": name}
        try:
            result = await _with_retries(lambda p=payload: _post("/video", p), attempts=2)
            return {
                "result": result,
                "provider": name,
                "degraded": idx > 0,
                "down": down,
                "used_fallback": idx > 0,
            }
        except ProviderUnavailable:
            await cache.mark_provider_down(name, 120)
            down.append(name)
            last_error = f"provider {name}: out of funds"
            continue
        except Exception as exc:
            down.append(name)
            last_error = f"provider {name}: {exc}"
            continue

    raise RuntimeError(last_error)
