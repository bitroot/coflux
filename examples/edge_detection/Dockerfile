FROM python:3.11
WORKDIR /app

ENV PYTHONPATH=/app

RUN apt-get update && \
    apt-get install -y libgl1-mesa-glx

COPY requirements.txt .
RUN pip install -r requirements.txt

ADD . /app

ENTRYPOINT ["coflux", "worker", "app.workflows"]
