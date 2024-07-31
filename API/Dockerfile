FROM python:3.9.19

WORKDIR /app

COPY . .

RUN apt-get update && apt-get upgrade -y \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

ENV FLASK_APP=app.py

CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]
