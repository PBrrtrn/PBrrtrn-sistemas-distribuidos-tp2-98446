FROM python:3.11.4

COPY test/test.sh /
COPY test/storage_handler /
COPY test/logs_reader /
COPY common /common
COPY backend /backend
ENTRYPOINT ["/bin/sh", "test.sh"]