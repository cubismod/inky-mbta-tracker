# inky-mbta-tracker

Inky MBTA Tracker is a personal project using the [Inky WHAT display](https://shop.pimoroni.com/products/inky-what?variant=21214020436051)
as a transit tracker for the Massachusetts Bay Transit Authority System.

## Features

- **Real-time Predictions**: Stream live predictions from the MBTA API
- **Static Schedules**: Fall back to static schedules when real-time data is unavailable
- **Vehicle Tracking**: Track real-time vehicle positions and status
- **Track Prediction**:  Predict commuter rail track assignments before they're announced
- **MQTT Integration**: Publish departure information to MQTT for home automation
- **Prometheus Metrics**: Monitor system performance and API usage

## Getting Started

You need a `.env` file with the following info:

```shell
# MBTA API Configuration
AUTH_TOKEN=<MBTA_API_TOKEN> # https://www.mbta.com/developers/v3-api

# Application Configuration
IMT_CONFIG=./config.json # optional to specify a different config file
IMT_LOG_FILE=./logs/inky.log # optional to also log to a file

# Redis Configuration (works for local dev w/ docker-compose)
IMT_REDIS_ENDPOINT=127.0.0.1
IMT_REDIS_PORT=6379
IMT_REDIS_PASSWORD=mbta # change this!

# MQTT Configuration
IMT_ENABLE_MQTT=true/false
IMT_MQTT_HOST=127.0.0.1
IMT_MQTT_USER=username
IMT_MQTT_PASS=mqtt_pass # change this!

# Performance Tuning
# API Timeouts
IMT_API_REQUEST_TIMEOUT=30 # API request timeout in seconds
IMT_TRACK_PREDICTION_TIMEOUT=15 # Track prediction timeout in seconds

# Feature Flags
IMT_RATE_LIMITING_ENABLED=true # Enable/disable rate limiting
IMT_SSE_ENABLED=true # Enable/disable Server-Sent Events
IMT_PROFILE_FILE=./profile.txt # optional to enable Yappi profiling around every hour

# Ollama AI Summarizer Configuration
OLLAMA_BASE_URL=http://localhost:11434 # Ollama API endpoint
OLLAMA_MODEL=llama3.2:1b # Model to use for summarization
OLLAMA_TIMEOUT=30 # Request timeout in seconds
OLLAMA_TEMPERATURE=0.1 # Model creativity (0.0=focused, 1.0=creative)
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

## Track Prediction Feature

The track prediction system analyzes historical track assignments to predict future track assignments for MBTA commuter rail trains before they are officially announced. This helps solve the "mad scramble" problem at major stations like South Station and North Station.

### How It Works

1. **Data Collection**: The system automatically captures track assignments from the MBTA API when trains arrive at stations
2. **Pattern Analysis**: Historical data is analyzed to identify patterns based on:
   - Headsign and destination
   - Time of day and day of week
   - Direction of travel
   - Route information
3. **Prediction Generation**: Before official track announcements, the system generates predictions with confidence scores
4. **Validation**: Predictions are validated against actual track assignments to improve accuracy over time

### Using Track Predictions

Track predictions are automatically integrated into the existing display system:

- **API Access**: Use the track prediction API to get detailed information:

  ```bash
  # Get predictions for a station
  curl http://localhost:8080/predictions/place-sstat

  # Get prediction statistics
  curl http://localhost:8080/stats/place-sstat/CR-Providence

  # Get historical data
  curl http://localhost:8080/historical/place-sstat/CR-Providence?days=30
  ```

### Configuration

Add this environment variable to enable track prediction features:

```shell
# Optional: Port for track prediction API (default: 8080)
IMT_TRACK_API_PORT=8080
```

#### Track Prediction Precaching

To enable automatic track prediction precaching, add these fields to your `config.json`:

```json
{
  "enable_track_predictions": true,
  "track_prediction_routes": ["CR-Worcester", "CR-Providence"],
  "track_prediction_stations": ["place-sstat", "place-north", "place-bbsta"],
  "track_prediction_interval_hours": 2
}
```

- `enable_track_predictions`: Boolean to enable/disable precaching (default: false)
- `track_prediction_routes`: List of commuter rail routes to precache (optional, defaults to all CR routes)
- `track_prediction_stations`: List of station IDs to precache for (optional, defaults to major stations)
- `track_prediction_interval_hours`: Hours between precaching runs (default: 2)

Note that you will also need to add the following stations to your configuration:

```json
  "stops": [
    {
      "stop_id": "place-north",
      "show_on_display": false,
      "transit_time_min": 1,
    },
    {
      "stop_id": "place-sstat",
      "show_on_display": false,
      "transit_time_min": 1
    },
    {
      "stop_id": "place-bbsta",
      "show_on_display": false,
      "transit_time_min": 1
    }
  ]
```

This creates real time departure/arrival trackers for these stations which is required to generate track predictions
but it doesn't show them on the Inky display or in MQTT.

## Architecture

```mermaid
flowchart TD
    subgraph inky-mbta-tracker
    B(schedule and prediction workers)
    D{{queue}}
    C(queue processor)
    TP[Track Predictor]
    AI[AI Summarizer]
    AQ{{AI Job Queue}}
    end
    B --> D
    C --> D
    B --> TP
    TP --> B
    B --> AI
    AI --> AQ
    AQ --> AI

    C -->E@{ shape: cyl, label: "Redis"}
    C -->F@{ shape: bow-rect, label: "MQTT" }
    TP --> E
    AI --> E
    AI --> O[Ollama API]

    G@{ shape: curv-trap, label: "inky-display" } -->|reads| E
    H[/Home Assistant/] -->|reads| F
    I[Track Prediction API] --> TP
    J[AI Summary API] --> AI
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
