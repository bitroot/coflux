FROM python:3.12

RUN apt-get update && \
    apt-get install -y libgl1-mesa-glx

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV PYTHONPATH=/app

COPY --from=coflux . ./coflux/

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

ADD . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

ENTRYPOINT ["uv", "run", "coflux", "worker"]
CMD ["app.workflows"]
