FROM python:3.11

ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_HOME=/usr/local \
    PYTHONPATH=/app

RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /app
COPY poetry.lock pyproject.toml ./

RUN poetry install --no-interaction --no-ansi

COPY . ./

ENTRYPOINT ["coflux", "worker", "slackbot.workflows"]
