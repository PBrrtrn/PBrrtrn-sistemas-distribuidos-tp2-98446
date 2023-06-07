FROM rabbitmq-python-base:0.0.1

COPY backend/data_ingestion_server /
COPY common /common
ENTRYPOINT ["/bin/sh"]