FROM bitnami/spark:latest

COPY src/ /opt/bitnami/spark/src/
COPY data/ /opt/bitnami/spark/data/
