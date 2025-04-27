FROM python:3.11
WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN python -m nltk.downloader punkt

ADD . /app

ENTRYPOINT ["coflux", "agent", "wikipedia.workflows"]
