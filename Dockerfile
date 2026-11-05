# syntax=docker/dockerfile:1

# Build stage: install the locked runtime dependencies and the project into a venv.
FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app
# Dependencies first: this layer is reused until pyproject.toml or uv.lock change.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

# Runtime stage: the venv, the migrations and a non-root user. No build tools.
FROM python:3.12-slim AS runtime
RUN useradd --create-home --uid 10001 hookgate
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY alembic.ini ./
COPY migrations ./migrations
# The provider simulator (demo profile of the compose file); it does not import hookgate.
COPY simulator ./simulator
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER hookgate
EXPOSE 8000
CMD ["uvicorn", "hookgate.api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]