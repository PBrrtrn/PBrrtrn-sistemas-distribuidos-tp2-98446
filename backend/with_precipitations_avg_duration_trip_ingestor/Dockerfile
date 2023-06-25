FROM rabbitmq-python-base:0.0.1

COPY backend/with_precipitations_avg_duration_trip_ingestor /
COPY common /common

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh"]