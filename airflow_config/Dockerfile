FROM apache/airflow


COPY requirements.txt ./
COPY webserver_config.py /opt/airflow/webserver_config.py


RUN pip install --no-cache-dir -r requirements.txt
RUN rm -rf requirements.txt

RUN apt-get install default-jdk
