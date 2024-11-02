FROM python:3.12

WORKDIR /app
COPY inky-mbta-tracker .

COPY requirements.txt .
COPY pyproject.toml .

RUN pip install -r requirements.txt

CMD ["python3", "inky-mbta-tracker/main.py"]