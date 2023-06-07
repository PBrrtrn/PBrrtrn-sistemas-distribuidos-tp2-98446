FROM rabbitmq-python-base:0.0.1

COPY backend/weather_filter /
COPY common /common
ENTRYPOINT ["/bin/sh"]