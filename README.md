# inky-mbta-tracker

Inky MBTA Tracker or IMT is a multi-purpose, async Python application tracking the
Massachusetts Bay Transit Authority. It relies on Redis for data storage in conjunction with
[inky-display](https://github.com/cubismod/inky-display) for e-inky display functionality.

## Configuring

### Getting Started

You need a `.env` file. Here are the supported options:

```shell
# MBTA API Configuration
AUTH_TOKEN=<MBTA_API_TOKEN> # https://www.mbta.com/developers/v3-api

# Application Configuration
IMT_CONFIG=./config.json # optional to specify a different config file
IMT_REDIS_BACKUP_TIME=21:50 # set to backup every night at this time, you are responsible for cleaning up backups

# prometheus exporter configuration
IMT_PROMETHEUS_ENABLE="true"  # disabled by default
IMT_PROM_PORT="8000"

# use these settings for real time self-monitoring health checks via prometheus
IMT_PROMETHEUS_JOB="imt_dev"
IMT_PROMETHEUS_ENDPOINT="http://prometheus.local"
IMT_METRIC_NAME="mbta_server_side_events:rate5m"

# Logging
IMT_LOG_FILE=./logs/inky.log # optional to also log to a file
LOG_LEVEL=INFO # DEBUG, INFO, WARNING, ERROR supported log levels
IMT_COLOR=true  # to log with colors

# monitoring with Pyroscope (optional)
IMT_PYROSCOPE_ENABLED=true
IMT_PYROSCOPE_HOST=http://pyroscope.local
IMT_PYROSCOPE_NAME=inky-local

# Redis Configuration (required)
IMT_REDIS_ENDPOINT=127.0.0.1
IMT_REDIS_PORT=6379
IMT_REDIS_PASSWORD=mbta # change this!

# MQTT Configuration
IMT_ENABLE_MQTT=true/false
IMT_MQTT_HOST=127.0.0.1
IMT_MQTT_USER=username
IMT_MQTT_PASS=mqtt_pass # change this!

# API Timeouts
IMT_API_REQUEST_TIMEOUT=30 # API request timeout in seconds
IMT_TRACK_PREDICTION_TIMEOUT=15 # Track prediction timeout in seconds

# Feature Flags
IMT_RATE_LIMITING_ENABLED=true # Enable/disable rate limiting
IMT_SSE_ENABLED=true # Enable/disable Server-Sent Events


```

## Prometheus & Grafana

Prometheus is available at port 8000.

You can use my [dashboard JSON](./grafana-dashboard.json) for a Grafana dashboard combining
the Prom metrics & a Loki datasource for logs.

![image of Inky MBTA Tracker Grafana dashboard](./img.png)

## Running

Spin up the local Redis server using the provided docker compose file.

Then run `task run` to start up the tracker.

Note: On Linux/macOS, `uvloop` is enabled automatically when available for faster asyncio performance. If not present, the default loop is used.

## Architecture

```mermaid
flowchart TD
    subgraph inky-mbta-tracker
    B(schedule and vehicle workers)
    D{{queue}}
    C(queue processor)
    end
    B --> D
    C --> D

    C -->E@{ shape: cyl, label: "Redis"}
    C -->F@{ shape: bow-rect, label: "MQTT" }

    G@{ shape: curv-trap, label: "inky-display" } -->|reads| E
    H[/Home Assistant/] -->|reads| F
```

At a base level, this project makes use of the MBTA V3 API, especially the [streaming API for predictions](https://www.mbta.com/developers/v3-api/streaming)
to setup individual workers for stops which are configured by the user. Optionally, a user can request static schedules via the
configuration file (explained below), and there is behavior that will retrieve static schedules if no real-time predictions are
available for a stop. From anecdotal experience the V3 streaming API appears to start dropping events after several hours
without any errors reported. Therefore, each stop watcher thread making use of the streaming API will restart after 1-3 hours
which is cleanly handled through the Python Async APIs.

This project works with [inky-display](https://github.com/cubismod/inky-display) which checks the Redis server a few times a minute
to refresh the display. Additionally, the departures can be integrated with [Home Assistant MQTT Sensors](https://www.home-assistant.io/integrations/sensor.mqtt/)
to create a real-time departure dashboard.
