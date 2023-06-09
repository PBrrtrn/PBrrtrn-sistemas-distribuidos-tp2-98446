FROM rabbitmq-python-base:0.0.1

COPY bully-test /
COPY common /common
ENTRYPOINT ["/bin/sh"]