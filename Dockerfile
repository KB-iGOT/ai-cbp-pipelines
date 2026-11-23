# syntax=docker/dockerfile:1
#
# Image for the Kafka pipeline workers and jobs in src/.
#
# Workers:  docker run <image> src.runner course_ingestion
#           docker run <image> src.runner course_retirement
# Jobs:     docker run <image> src.jobs.replay_dlq <pipeline> --dry-run

ARG PYTHON_VERSION=3.12

# ── Build stage: resolve dependencies from uv.lock ───────────────────────────
FROM python:${PYTHON_VERSION}-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --no-cache

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:${PATH}"

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY ./src /app/src

# Heartbeat file for the liveness probe is written to /tmp, which is writable for this user.
USER 10001

ENTRYPOINT ["python", "-m"]
CMD ["src.runner", "--list"]
