FROM rabbitmq-python-base:0.0.1

COPY frontend /
COPY common /common
ENTRYPOINT ["/bin/sh"]