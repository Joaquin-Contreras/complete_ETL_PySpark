FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    netcat-openbsd

WORKDIR /app

# Instalar dependencias Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY lib/mysql-connector-j-9.4.0.jar /opt/bitnami/spark/jars/
COPY . /app

COPY wait-for-it.sh /app/wait-for-it.sh
RUN chmod +x /app/wait-for-it.sh

RUN chown -R 1001:1001 /app

RUN mkdir -p /app/artifacts && chown -R 1001:1001 /app/artifacts
USER 1001

CMD ["/app/wait-for-it.sh", "mysql", "3306", "--", "spark-submit","--master", "local[*]","spark_jobs/run_pipeline.py"]
