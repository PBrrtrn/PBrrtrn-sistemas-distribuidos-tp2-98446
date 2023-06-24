FROM rabbitmq-python-base:0.0.1

COPY backend/stations_manager /
COPY common /common

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh"]