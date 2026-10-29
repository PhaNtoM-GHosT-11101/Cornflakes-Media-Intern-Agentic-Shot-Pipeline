import os

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://demo:demo@postgres:5432/demo")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
MOCKS_URL = os.environ.get("MOCKS_URL", "http://mocks:9000")

USER_ID = os.environ.get("DEMO_USER", "demo")

# Consistency threshold (calibrated by hand for the demo; see README).
CONSISTENCY_THRESHOLD = float(os.environ.get("CONSISTENCY_THRESHOLD", "0.75"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))
LEASE_SECONDS = 30
HEARTBEAT_SECONDS = 10
