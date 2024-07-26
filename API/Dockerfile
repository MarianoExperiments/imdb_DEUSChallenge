FROM python:latest

WORKDIR /app

COPY . .

RUN chmod +x setup_datalake.sh 


RUN ./setup_datalake.sh \ 
    && pip install --no-cache-dir -r requirements.txt


CMD ["python", "app.py"]
