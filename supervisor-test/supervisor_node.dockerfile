FROM rabbitmq-python-base:0.0.1
RUN pip install docker

COPY supervisor-test /
COPY common /common
ENTRYPOINT ["/bin/sh"]