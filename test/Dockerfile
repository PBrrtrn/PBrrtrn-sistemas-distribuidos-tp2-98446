FROM python:3.11.4

COPY test/test.sh /
COPY test/storage_handler /
COPY common /common
COPY backend /backend
ENTRYPOINT ["/bin/sh", "test.sh"]