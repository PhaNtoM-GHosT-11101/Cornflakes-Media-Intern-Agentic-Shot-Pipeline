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

# Admission control (Redis, per user, cross-process).
RATE_LIMIT_PER_MIN = int(os.environ.get("RATE_LIMIT_PER_MIN", "30"))
ACTIVE_JOB_LIMIT = int(os.environ.get("ACTIVE_JOB_LIMIT", "2"))

# Worker concurrency pools (Redis counting semaphores).
IO_CONCURRENCY = int(os.environ.get("IO_CONCURRENCY", "8"))    # provider HTTP calls
CPU_CONCURRENCY = int(os.environ.get("CPU_CONCURRENCY", "2"))  # compose / transcode
