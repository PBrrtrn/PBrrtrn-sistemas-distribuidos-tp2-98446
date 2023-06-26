docker build -f test/Dockerfile -t tests-image:latest .
docker run tests-image:latest