# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development Commands (using Task runner)
- `task run` - Start the main MBTA tracker application
- `task prediction-api` - Start the track prediction API server
- `task test` - Run pytest test suite
- `task check` - Run linting (ruff) and type checking (mypy)
- `task fix` - Auto-fix ruff linting issues
- `task format` - Format code with ruff
- `task compute-class-hashes` - Update class hashes for schema versioning

### Direct Commands
- `uv run inky-mbta-tracker` - Run main application directly
- `uv run inky-mbta-tracker --prediction-api` - Run prediction API directly
- `uv run pytest` - Run tests directly
- `uv run ruff check inky-mbta-tracker` - Lint check
- `uv run mypy` - Type check

## Architecture

This is a Python-based real-time transit tracking system that monitors MBTA (Massachusetts Bay Transit Authority) schedules and vehicle positions. The application consists of several key components:

### Core Components

**Main Application (`main.py`)**
- Entry point that orchestrates multiple worker threads
- TaskTracker system manages thread lifecycle with automatic restarts (45-120 min intervals)
- Queue-based event processing system
- Prometheus metrics server on port 8000

**Schedule Tracker (`schedule_tracker.py`)**
- Processes events from the queue and stores data in Redis
- Handles both schedule events and vehicle position updates
- Publishes data to MQTT for home automation integration
- Implements Redis locking for concurrent access

**MBTA Client (`mbta_client.py`)**
- Interfaces with MBTA V3 API and streaming endpoints
- Creates worker threads for different task types (predictions, schedules, vehicles)
- Handles API rate limiting and connection management

**Track Predictor (`track_predictor/`)**
- NEW feature that predicts commuter rail track assignments
- Analyzes historical patterns to predict tracks before official announcements
- Separate FastAPI service that can run independently
- Stores predictions and historical data in Redis with TTL

### Data Flow

1. Worker threads fetch data from MBTA streaming API
2. Events are queued for processing
3. Queue processors store data in Redis and publish to MQTT
4. Track predictor analyzes patterns and generates predictions
5. External systems (inky-display, Home Assistant) consume data from Redis/MQTT

### Configuration

The application uses a `config.json` file to define stops to track, with options for:
- Real-time predictions vs. static schedules
- Route and direction filtering  
- Transit time calculations
- Display preferences

### Dependencies

- **Redis**: Primary data store with TTL-based cleanup
- **MQTT**: Optional pub/sub for home automation
- **FastAPI**: Track prediction API
- **Prometheus**: Metrics collection
- **uv**: Package management and task runner

### Testing

Tests are located in `inky-mbta-tracker/tests/` and focus on:
- MBTA client functionality
- Schedule tracking logic
- Use pytest with async support

### Code Style

- Uses ruff for linting and formatting (line length: 88)
- MyPy for type checking with strict settings
- Python 3.13+ required
- Black-compatible formatting style

MBTA API available at <https://api-v3.mbta.com/docs/swagger/swagger.json>
