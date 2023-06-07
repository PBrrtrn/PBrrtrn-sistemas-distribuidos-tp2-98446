FROM rabbitmq-python-base:0.0.1

COPY backend/distance_calculator /
COPY common /common

RUN pip install -r requirements.txt
ENTRYPOINT ["/bin/sh"]