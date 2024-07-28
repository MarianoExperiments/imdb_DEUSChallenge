FROM apache/airflow


COPY requirements.txt ./
COPY webserver_config.py /opt/airflow/webserver_config.py


RUN pip install --no-cache-dir -r requirements.txt
RUN rm -rf requirements.txt


USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow