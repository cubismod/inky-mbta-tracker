# inky-mbta-tracker

Inky MBTA Tracker is a personal project using the [Inky WHAT display](https://shop.pimoroni.com/products/inky-what?variant=21214020436051)
as a transit tracker for the Massachusetts Bay Transit Authority System.

## Features

- **Real-time Predictions**: Stream live predictions from the MBTA API
- **Static Schedules**: Fall back to static schedules when real-time data is unavailable
- **Vehicle Tracking**: Track real-time vehicle positions and status
- **Track Prediction**: **NEW!** Predict commuter rail track assignments before they're announced
- **AI Alert Summarizer**: **NEW!** Generate intelligent summaries of MBTA alerts using Ollama
- **MQTT Integration**: Publish departure information to MQTT for home automation
- **Prometheus Metrics**: Monitor system performance and API usage

## Getting Started

You need a `.env` file with the following info:

```shell
# MBTA API Configuration
AUTH_TOKEN=<MBTA_API_TOKEN> # https://www.mbta.com/developers/v3-api

# Application Configuration
IMT_CONFIG=./config.json # optional to specify a different config file

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
IMT_PROCESS_QUEUE_THREADS=1 # Number of async consumer tasks
IMT_PIPELINE_BATCH=200 # Base batch size per consumer flush
IMT_PIPELINE_TIMEOUT_SEC=0.2 # Base flush timeout in seconds
IMT_QUEUE_BACKPRESSURE_RATIO=0.8 # Queue capacity considered "high"
IMT_QUEUE_BACKPRESSURE_SUSTAIN_SEC=5 # Seconds above high-water mark before warning
IMT_QUEUE_BACKPRESSURE_RELOG_SEC=60 # Minimum seconds between warnings

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

# Ollama Performance Tuning (Intel i5-9400 optimized)
OLLAMA_NUM_PARALLEL=2 # Limit concurrent requests
OLLAMA_MAX_LOADED_MODELS=1 # Keep memory usage low
OLLAMA_FLASH_ATTENTION=1 # Enable if supported by your hardware
```

## AI Alert Summarization

The MBTA Tracker now includes AI-powered alert summarization using Ollama, a local LLM server. This feature provides intelligent, human-readable summaries of transit alerts.

### AI Features

- **Group Summaries**: Generate comprehensive summaries of all active alerts together
- **Individual Alert Summaries**: Generate detailed summaries for each individual alert
- **Adaptive Prompting**: Choose between comprehensive (detailed) and concise (1-2 sentences per alert) summary styles
- **Multiple Output Formats**: Support for Text, Markdown, and JSON output
- **Intelligent Caching**: Redis-based caching with configurable TTL
- **Background Processing**: Asynchronous job queue with priority-based processing
- **Conversational Cleanup**: Automatic removal of conversational phrases from LLM responses
- **Periodic Refresh**: Automatic generation of summaries every 30 minutes
- **Startup Summaries**: Immediate summary generation on application startup

### Configuration

#### Environment Variables (Highest Priority)

```bash
# Required
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.2:3b

# Optional
OLLAMA_TIMEOUT=60
OLLAMA_TEMPERATURE=0.2
OLLAMA_MAX_RETRIES=3
OLLAMA_CACHE_TTL=1800
```

#### config.json (Fallback)

```json
{
  "ollama": {
    "enabled": true,
    "base_url": "http://localhost:11434",
    "model": "llama3.2:3b",
    "timeout": 60,
    "temperature": 0.2,
    "max_retries": 3,
    "cache_ttl": 1800
  }
}
```

#### Configuration Priority

1. **Environment Variables** (highest priority)
2. **config.json** (fallback)
3. **Hardcoded Defaults** (lowest priority)

### Requirements

- **Ollama**: Local LLM server running with a compatible model
- **Compatible Model**: Models like `llama3.2:3b`, `gemma3:1b`, or similar
- **Memory**: Sufficient RAM for the chosen model (typically 4-8GB for 3B models)

### How It Works

1. **Collection**: Fetches current MBTA alerts via the MBTA API
2. **AI Processing**: Sends formatted alert data to Ollama for summarization
3. **Response Cleaning**: Removes conversational phrases and unnecessary text
4. **Caching**: Stores summaries in Redis with configurable TTL
5. **Integration**: Provides both group and individual alert summaries
6. **Periodic Updates**: Automatically refreshes summaries every 30 minutes

### API Endpoints

#### Group Summaries

- `GET /ai/summaries/active` - Get summary of all active alerts
- `POST /ai/summaries/bulk` - Force generation of group summaries with style selection
- `POST /ai/summaries/generate` - Generate summaries with custom style and format
- `POST /alerts/summarize` - Manual group summarization

#### Individual Alert Summaries

- `GET /ai/summaries/individual/{alert_id}` - Get individual alert summary with configurable sentence limit
- `POST /ai/summaries/individual/generate` - Force individual summary generation with style and sentence limit options

#### Utility Endpoints

- `GET /ai/test` - Test AI summarizer functionality
- `GET /ai/status` - Check summarizer health and status
- `GET /ai/summaries/cached` - View all cached summaries

#### Output Formats

All summary endpoints support three output formats:

- `text` (default): Plain text summaries
- `markdown`: Formatted markdown with headers and structure
- `json`: Structured JSON output

#### Summary Styles

Choose between two summary styles:

- `comprehensive` (default): Detailed analysis with full coverage of each alert, including impact analysis, recommendations, and systemic patterns
- `concise`: Brief summaries with 1-2 sentences per alert, focusing on essential information for quick understanding

#### Sentence Limits

For individual alert summaries, control the maximum number of sentences:

- `1`: Ultra-concise single sentence summaries
- `2+`: Standard concise summaries with multiple sentences
- Range: 1-5 sentences maximum

### Example Usage

#### Generate Group Summary

```bash
curl "http://localhost:8000/ai/summaries/active?format=markdown"
```

#### Generate Concise Summary

```bash
curl -X POST "http://localhost:8000/ai/summaries/bulk?style=concise&format=markdown"
```

#### Generate Comprehensive Summary

```bash
curl -X POST "http://localhost:8000/ai/summaries/bulk?style=comprehensive&format=json"
```

#### Generate Individual Summary

```bash
# Standard 2-sentence summary
curl -X POST "http://localhost:8000/ai/summaries/individual/generate?alert_id=12345&format=json"

# Ultra-concise 1-sentence summary
curl -X POST "http://localhost:8000/ai/summaries/individual/generate?alert_id=12345&sentence_limit=1&format=markdown"
```

#### Manual Summarization

```bash
curl -X POST "http://localhost:8000/alerts/summarize" \
  -H "Content-Type: application/json" \
  -d '{
    "alerts": [...],
    "format": "markdown"
  }'
```

### Monitoring

The system includes Prometheus metrics for monitoring:

- `ai_summarization_requests_total`: Total summarization requests
- `ai_summarization_errors_total`: Total summarization errors
- `ai_summarization_duration_seconds`: Summarization processing time
- `ai_summarization_cache_hits_total`: Cache hit count

### Background Tasks

- **Group Summary Refresh**: Every 30 minutes, generates new summaries for all active alerts
- **Individual Summary Generation**: Every 30 minutes, generates individual summaries for each alert (1-sentence ultra-concise format)
- **Startup Summaries**: Immediately generates summaries on application startup
- **Sequential Processing**: All Ollama requests are processed one at a time to prevent overwhelming the AI service
- **Intelligent Backoff**: CPU-aware job processing with adaptive delays

### Troubleshooting

- **Model Not Found**: Ensure the specified model is available in Ollama
- **Memory Issues**: Use smaller models (1B instead of 3B) for limited RAM
- **Connection Errors**: Verify Ollama is running and accessible
- **Truncated Responses**: Check model configuration and increase context length if needed

### Monitoring

Prometheus metrics are available for monitoring:

- `ai_summarization_requests_total`: Total summarization requests
- `ai_summarization_errors_total`: Total summarization errors
- `ai_summarization_duration_seconds`: Summarization processing time
- `ai_summarization_cache_hits_total`: Cache hit count

### Performance Tuning (Async Consumers)

The app runs producers and consumers on a single asyncio event loop. You can tune throughput and latency with these environment variables:

- `IMT_PROCESS_QUEUE_THREADS`: Number of async consumer tasks that flush events to Redis (default: `1`). Increase to `2–4` if you observe backpressure warnings.
- `IMT_PIPELINE_BATCH`: Base batch size per consumer flush (default: `200`). The effective batch automatically scales up to 5x when the queue is full.
- `IMT_PIPELINE_TIMEOUT_SEC`: Base flush timeout in seconds (default: `0.2`). Timeout adapts down as the queue fills to keep latency reasonable.
- `IMT_QUEUE_BACKPRESSURE_RATIO`: Fraction of the queue capacity considered “high” (default: `0.8`).
- `IMT_QUEUE_BACKPRESSURE_SUSTAIN_SEC`: Seconds that queue depth must remain above the high-water mark before logging a warning (default: `5`).
- `IMT_QUEUE_BACKPRESSURE_RELOG_SEC`: Minimum seconds between repeated backpressure warnings (default: `60`).

Consumers batch and coalesce events before writing to Redis to reduce load:

- Vehicles are coalesced by `vehicle id` to the last update within the batch.
- Schedule events are coalesced by “trip key” `(trip_id, stop)` to the last update within the batch. Events that lack trip context (including most removals) are coalesced by `event id`.
- MQTT publishing is offloaded to a background thread to avoid blocking the event loop.

If backpressure warnings persist, consider raising `IMT_PROCESS_QUEUE_THREADS`, increasing `IMT_PIPELINE_BATCH`, or reviewing Redis/network capacity.

From there, create a `config.json` like so to the following schema:

```json5
{
  "stops": [
    {
      // REQUIRED, the stop ID which can be retrieved from the stop page like this example:
      // https://www.mbta.com/stops/place-davis
      "stop_id": "place-davis",
      // OPTIONAL, filter only arrivals going in this direction, typically 1 means that inbound
      // and 0 means outbound but that depends on the route
      // more info here: https://api-v3.mbta.com/docs/swagger/index.html#/Prediction/ApiWeb_PredictionController_index
      "direction_filter": "1",
      // OPTIONAL, filter only arrivals for the following route ID, useful if a subway station has
      // a bunch of bus routes that you don't care to track for example
      "route_filter": "Red",
      // REQUIRED, time to walk/drive/bike/etc to get to this station
      // this will be used by the display component to actually determine when
      // you can make an arrival
      "transit_time_min": 18,
      // OPTIONAL, use this for stops that never have real-time departure information
      // (looking at you with side-eye, Medford-Tufts. This will spawn a different
      // task which retrieves static schedule information every couple of hours
      "show_on_display": true,
      // OPTIONAL, set to false if you don't want this stop to show up on the Inky display
    }
  ]
}
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

## AI Alert Summarizer Feature

The AI Alert Summarizer uses local AI models via Ollama to generate intelligent, concise summaries of MBTA alerts. This feature helps users quickly understand the impact and scope of multiple alerts without having to read through each one individually.

### How It Works

1. **Alert Collection**: The system collects current MBTA alerts from the API
2. **Job Queuing**: Summary requests are queued with intelligent backoff to prevent CPU overload
3. **AI Processing**: Alerts are formatted and sent to an Ollama endpoint for summarization
4. **Smart Summarization**: The AI model identifies common themes, patterns, and key information
5. **Redis Storage**: Summaries are automatically stored in Redis for fast retrieval
6. **Caching**: Smart caching prevents duplicate work and improves response times

### Prerequisites & Server Setup

#### 1. Install Ollama on Your Server

**For Linux (Ubuntu/Debian):**

```bash
# Download and install Ollama
curl -fsSL https://ollama.ai/install.sh | sh

# Verify installation
ollama --version
```

**For other systems:**

- macOS: Download from [ollama.ai](https://ollama.ai/download)
- Windows: Download from [ollama.ai](https://ollama.ai/download)

#### 2. Configure Ollama as a Service

**Enable Ollama to start automatically:**

```bash
# Enable Ollama service (Linux with systemd)
sudo systemctl enable ollama
sudo systemctl start ollama

# Check service status
sudo systemctl status ollama
```

**Configure Ollama for external access (if needed):**

```bash
# Edit Ollama service configuration
sudo systemctl edit ollama

# Add these lines to allow external connections:
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"

# Restart the service
sudo systemctl restart ollama
```

#### 3. Pull and Test the Model

**For CPU-only systems (like your Intel i5-9400):**

```bash
# Pull the recommended lightweight model
ollama pull llama3.2:1b

# Test the model installation
ollama run llama3.2:1b "Summarize this: The sky is blue"

# Expected output: A brief summary about the sky's color
```

**For systems with more resources:**

```bash
# Alternative models (choose one)
ollama pull llama3.2:3b    # Better quality, slower
ollama pull gemma2:2b      # Good balance
ollama pull qwen2.5:0.5b   # Very fast
```

#### 4. Verify Ollama API Access

```bash
# Test API endpoint
curl http://localhost:11434/api/tags

# Should return JSON with available models
# If this fails, check firewall and service status
```

#### 5. Performance Optimization

**For Intel i5-9400 (6 cores, 4.1GHz):**

```bash
# Set environment variables for optimal performance
export OLLAMA_NUM_PARALLEL=2        # Limit concurrent requests
export OLLAMA_MAX_LOADED_MODELS=1   # Keep memory usage low
export OLLAMA_FLASH_ATTENTION=1     # Enable if supported

# Make these permanent by adding to ~/.bashrc or /etc/environment
echo "OLLAMA_NUM_PARALLEL=2" >> ~/.bashrc
echo "OLLAMA_MAX_LOADED_MODELS=1" >> ~/.bashrc
```

#### 6. Firewall Configuration (if using external access)

```bash
# Allow Ollama through firewall (Ubuntu/Debian)
sudo ufw allow 11434/tcp

# For other systems, ensure port 11434 is open
```

#### 7. Troubleshooting Common Issues

**Ollama service won't start:**

```bash
# Check service logs
sudo journalctl -u ollama -f

# Common fixes:
sudo systemctl daemon-reload
sudo systemctl restart ollama
```

**Model download fails:**

```bash
# Check disk space
df -h

# Clear Ollama cache if needed
rm -rf ~/.ollama/models/*
ollama pull llama3.2:1b
```

**API not accessible:**

```bash
# Check if Ollama is listening
netstat -tlnp | grep 11434

# Test local connection
curl -v http://localhost:11434/api/version
```

**High CPU usage:**

```bash
# Monitor Ollama process
top -p $(pgrep ollama)

# Reduce concurrent jobs in your config.json:
{
  "ollama": {
    "enabled": true,
    "base_url": "http://localhost:11434",
    "model": "llama3.2:1b",
    "timeout": 60,
    "temperature": 0.1
  }
}
```

#### 8. Production Deployment Considerations

**Resource Monitoring:**

```bash
# Monitor Ollama resource usage
watch -n 5 'ps aux | grep ollama && free -h && df -h'

# Set up log rotation for Ollama logs
sudo tee /etc/logrotate.d/ollama << EOF
/var/log/ollama/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
EOF
```

**Systemd Service Configuration:**

```bash
# Create custom Ollama service configuration
sudo tee /etc/systemd/system/ollama.service.d/custom.conf << EOF
[Service]
# Limit memory usage (adjust for your system)
MemoryLimit=8G

# Set environment variables
Environment="OLLAMA_HOST=0.0.0.0:11434"
Environment="OLLAMA_NUM_PARALLEL=2"
Environment="OLLAMA_MAX_LOADED_MODELS=1"

# Restart policy
Restart=always
RestartSec=10

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/ollama
EOF

# Reload and restart
sudo systemctl daemon-reload
sudo systemctl restart ollama
```

**Health Monitoring Script:**

```bash
# Create Ollama health check script
sudo tee /usr/local/bin/ollama-health.sh << 'EOF'
#!/bin/bash
ENDPOINT="http://localhost:11434"
TIMEOUT=10

if curl -s --max-time $TIMEOUT "$ENDPOINT/api/tags" > /dev/null; then
    echo "$(date): Ollama API is healthy"
    exit 0
else
    echo "$(date): Ollama API is not responding"
    # Restart Ollama service
    systemctl restart ollama
    exit 1
fi
EOF

sudo chmod +x /usr/local/bin/ollama-health.sh

# Add to crontab for automatic monitoring
echo "*/5 * * * * /usr/local/bin/ollama-health.sh >> /var/log/ollama-health.log 2>&1" | sudo crontab -
```

#### 9. Quick Start (TL;DR)

**For impatient users who just want it working:**

```bash
# Install and start Ollama
curl -fsSL https://ollama.ai/install.sh | sh
sudo systemctl enable ollama
sudo systemctl start ollama

# Pull the model
ollama pull llama3.2:1b

# Test it works
curl http://localhost:11434/api/tags

# Add to your config.json
{
  "ollama": {
    "enabled": true,
    "model": "llama3.2:1b"
  }
}

# Start your MBTA tracker and you're done!
```

**Verification that everything works:**

```bash
# Test the summarizer endpoint
curl -X POST http://localhost:8080/alerts/summarize \
  -H "Content-Type: application/json" \
  -d '{"alerts": [], "max_length": 100}'

# Expected: {"summary": "No alerts to summarize.", ...}
```

### Job Queue System

The AI summarizer includes an intelligent job queue system designed specifically for CPU-limited environments:

#### Features

- **CPU-Aware Processing**: Limited to 2 concurrent jobs by default to prevent overwhelming your CPU
- **Priority System**: Four priority levels (LOW, NORMAL, HIGH, URGENT) for different types of alerts
- **Intelligent Backoff**: Adaptive delays based on system load and queue depth
- **Automatic Retry**: Failed jobs are retried with exponential backoff (up to 3 attempts)
- **Redis Integration**: All summaries are automatically stored and retrieved from Redis

#### Job Priority Levels

- **URGENT** (4): Emergency alerts requiring immediate processing
- **HIGH** (3): New critical alerts (severity 7+)
- **NORMAL** (2): Regular alert updates
- **LOW** (1): Background refresh, non-urgent summaries

#### Queue Management

The system automatically:

- Checks Redis cache before creating new jobs
- Manages queue size (max 100 jobs) to prevent memory issues
- Cleans up completed jobs (keeps last 100)
- Adapts processing speed based on system load

#### Backoff Strategy

Optimized for Intel i5-9400 CPU:

- **Base delay**: 5 seconds between jobs
- **Maximum delay**: 60 seconds during heavy load
- **Exponential backoff**: 1.5x multiplier for failed jobs
- **Load-based adaptation**: Longer delays when queue is full

### Configuration

Add Ollama configuration to your `config.json`:

```json
{
  "ollama": {
    "enabled": true,
    "base_url": "http://localhost:11434",
    "model": "llama3.2:1b",
    "timeout": 30,
    "temperature": 0.1,
    "cache_ttl": 300
  }
}
```

**Configuration Priority**: Environment variables (like `OLLAMA_MODEL`) take precedence over `config.json` settings. This allows you to:

- Use `config.json` for default values
- Override specific settings via environment variables
- Deploy the same config file across different environments
- Easily change settings without modifying files

**Configuration Options:**

- `enabled`: Enable/disable the AI summarizer feature
- `base_url`: Ollama API endpoint (default: <http://localhost:11434>)
- `model`: Model name to use for summarization
- `timeout`: Request timeout in seconds
- `temperature`: Model creativity (0.0 = focused, 1.0 = creative)
- `cache_ttl`: Summary cache TTL in seconds

### Environment Variables

You can also configure Ollama via environment variables. These are the same variables documented in the `.env` section above:

```shell
# Ollama Configuration
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.2:1b
OLLAMA_TIMEOUT=30
OLLAMA_TEMPERATURE=0.1

# Performance Tuning (Intel i5-9400 optimized)
OLLAMA_NUM_PARALLEL=2
OLLAMA_MAX_LOADED_MODELS=1
OLLAMA_FLASH_ATTENTION=1
```

**Note**: Environment variables take precedence over `config.json` settings. This allows you to override configuration for different deployment environments (development, staging, production) without changing the config file.

### Redis Storage

The AI summarizer automatically stores all generated summaries in Redis for efficient caching:

#### Storage Details

- **Cache Keys**: `ai_summary:{alerts_hash}` - deterministic hash based on alert content
- **TTL**: 1 hour (3600 seconds) - automatically expires old summaries
- **Hash Generation**: Based on alert IDs, headers, severity, and effects
- **Lookup Mapping**: `alerts_to_summary:{alerts_hash}` for quick cache lookups

#### Storage Benefits

- **Fast Retrieval**: Cached summaries return instantly without AI processing
- **Reduced Load**: Prevents duplicate AI calls for identical alert sets
- **Persistence**: Summaries survive application restarts
- **Memory Efficient**: Automatic expiration prevents Redis memory bloat

### API Endpoints

The AI summarizer provides multiple endpoints for different use cases:

#### Summarize Alerts

```bash
POST /alerts/summarize
Content-Type: application/json

{
  "alerts": [],  # Optional: specific alerts to summarize
  "max_length": 200,  # Optional: maximum summary length
  "include_route_info": true,  # Optional: include route details
  "include_severity": true  # Optional: include severity levels
}
```

**Response:**

```json
{
  "summary": "Multiple delays affecting Red Line service due to signal problems at Park Street. Expect 10-15 minute delays on inbound trains. Green Line experiencing minor delays due to construction at Copley.",
  "alert_count": 3,
  "model_used": "llama3.2:1b",
  "processing_time_ms": 2450.5
}
```

#### Health Check

```bash
GET /alerts/summarize/health
```

**Response:**

```json
{
  "status": "healthy",
  "model": "llama3.2:1b",
  "endpoint": "http://localhost:11434",
  "enabled": true
}
```

#### Queue Management

```bash
# Queue a summary job for background processing
POST /alerts/summarize/queue
Content-Type: application/json

{
  "alerts": [],  # Required: alerts to summarize
  "priority": "NORMAL",  # Optional: LOW, NORMAL, HIGH, URGENT
  "config": {
    "max_length": 300,
    "include_route_info": true,
    "include_severity": true
  }
}
```

**Response:**

```json
{
  "job_id": "summary_1642123456789_001",
  "status": "pending",
  "message": "Job queued successfully"
}
```

#### Job Status

```bash
# Check the status of a queued job
GET /alerts/summarize/job/{job_id}
```

**Response:**

```json
{
  "job_id": "summary_1642123456789_001",
  "status": "completed",
  "created_at": "2024-01-15T10:30:00Z",
  "completed_at": "2024-01-15T10:30:05Z",
  "result": "Multiple service disruptions affecting Red and Green lines...",
  "alert_count": 5,
  "retry_count": 0
}
```

**Job Status Values:**

- `pending`: Job is queued and waiting to be processed
- `processing`: Job is currently being processed by AI
- `completed`: Job finished successfully
- `failed`: Job failed after all retries
- `cancelled`: Job was cancelled before processing

### Model Recommendations

**For CPU-only systems (like yours):**

- **`llama3.2:1b`** - Fast, efficient, good quality (recommended)
- **`gemma2:2b`** - Good balance of speed and quality
- **`qwen2.5:0.5b`** - Very fast, decent quality

**For systems with GPU support:**

- **`llama3.2:3b`** - Better quality, slower on CPU
- **`llama3.2:8b`** - High quality, requires more resources

### Performance Expectations

**With Intel i5-9400 (6 cores, 4.1GHz):**

- **Response time**: 2-5 seconds for alert summaries
- **Memory usage**: ~2-3GB RAM when active
- **CPU usage**: 60-80% during generation

**With GPU acceleration:**

- **Response time**: 0.5-2 seconds
- **Memory usage**: ~4-8GB VRAM + 2-3GB RAM
- **GPU usage**: 80-95% during generation

### Data Storage

The system stores various types of data in Redis with different retention policies:

#### Track Prediction Data

- **Track assignments**: 6 months
- **Predictions**: 1 week
- **Statistics**: 30 days

#### AI Summary Data

- **Alert summaries**: 1 hour (automatic cache expiration)
- **Summary jobs**: 24 hours (for job status tracking)
- **Alert hashes**: 1 hour (for quick cache lookups)

#### Cache Keys

- Track data: `track_history:*`, `track_prediction:*`, `track_stats:*`
- AI summaries: `ai_summary:*`, `alerts_to_summary:*`
- Job tracking: `summary_job:*`

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
