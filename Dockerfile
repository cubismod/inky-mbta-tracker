FROM python:3.12 AS builder

COPY requirements.txt .
RUN pip install --user -r requirements.txt

FROM python:3.12-slim as main

WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY inky-mbta-tracker/ ./inky-mbta-tracker/

COPY pyproject.toml .
ENV PATH=/root/.local:$PATH

RUN python3 -m ruff check inky-mbta-tracker/

CMD ["python3", "inky-mbta-tracker/main.py"]