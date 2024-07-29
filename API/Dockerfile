FROM python:3.9.19

WORKDIR /app

COPY . .

RUN apt-get update && apt-get upgrade -y \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt


CMD ["python", "app.py"]

