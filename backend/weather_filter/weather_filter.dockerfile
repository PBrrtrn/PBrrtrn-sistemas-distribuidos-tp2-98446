FROM rabbitmq-python-base:0.0.1

COPY backend/weather_filter /
COPY common /common

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh"]