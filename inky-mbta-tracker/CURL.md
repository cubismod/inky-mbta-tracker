# MBTA Transit Data API - CURL Examples

This document provides examples of how to interact with the MBTA Transit Data API using `curl` commands.

## Base URL

The API is available at:

- **Production**: `https://imt.ryanwallace.cloud`
- **Local Development**: `http://localhost:8080`

## Authentication

Most endpoints are publicly accessible, but some have rate limiting:

- **Standard endpoints**: 50-100 requests per minute
- **AI endpoints**: 10-30 requests per minute
- **Streaming endpoints**: 20 requests per minute

## Health Check

### Check API Status

```bash
curl -X GET "https://imt.ryanwallace.cloud/health"
```

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00.123456"
}
```

## Track Predictions

### Generate Single Track Prediction

```bash
curl -X POST "https://imt.ryanwallace.cloud/predictions" \
  -H "Content-Type: application/json" \
  -d '{
    "station_id": "place-north",
    "route_id": "CR-Providence",
    "trip_id": "trip-123",
    "headsign": "South Station",
    "direction_id": 0,
    "scheduled_time": "2024-01-15T10:30:00"
  }'
```

### Generate Multiple Track Predictions

```bash
curl -X POST "https://imt.ryanwallace.cloud/chained-predictions" \
  -H "Content-Type: application/json" \
  -d '{
    "predictions": [
      {
        "station_id": "place-north",
        "route_id": "CR-Providence",
        "trip_id": "trip-123",
        "headsign": "South Station",
        "direction_id": 0,
        "scheduled_time": "2024-01-15T10:30:00"
      },
      {
        "station_id": "place-south",
        "route_id": "CR-Providence",
        "trip_id": "trip-456",
        "headsign": "Providence",
        "direction_id": 1,
        "scheduled_time": "2024-01-15T11:00:00"
      }
    ]
  }'
```

### Get Prediction Statistics

```bash
curl -X GET "https://imt.ryanwallace.cloud/stats/place-north/CR-Providence"
```

### Get Historical Track Assignments

```bash
curl -X GET "https://imt.ryanwallace.cloud/historical/place-north/CR-Providence?days=30"
```

## Vehicle Data

### Get Current Vehicle Positions

```bash
curl -X GET "https://imt.ryanwallace.cloud/vehicles"
```

**Note**: This endpoint returns a large GeoJSON response. Consider using the streaming endpoint for real-time updates.

### Get Vehicle Positions as JSON File

```bash
curl -X GET "https://imt.ryanwallace.cloud/vehicles.json" \
  -H "Accept: application/json"
```

### Stream Vehicle Positions (Server-Sent Events)

```bash
curl -X GET "https://imt.ryanwallace.cloud/vehicles/stream?poll_interval=2.0&heartbeat_interval=30.0" \
  -H "Accept: text/event-stream"
```

**Query Parameters:**

- `poll_interval`: Poll interval in seconds (0.1 to 10.0, default: 1.0)
- `heartbeat_interval`: Heartbeat interval in seconds (1.0 to 120.0, default: 15.0)
- `route`: Filter by route (can be repeated for multiple routes)

**Example with route filtering:**

```bash
curl -X GET "https://imt.ryanwallace.cloud/vehicles/stream?route=Red&route=Orange" \
  -H "Accept: text/event-stream"
```

## Alerts

### Get Current MBTA Alerts

```bash
curl -X GET "https://imt.ryanwallace.cloud/alerts"
```

### Get Alerts as JSON File

```bash
curl -X GET "https://imt.ryanwallace.cloud/alerts.json" \
  -H "Accept: application/json"
```

## Route Shapes

### Get Route Shapes

```bash
curl -X GET "https://imt.ryanwallace.cloud/shapes"
```

### Get Route Shapes as JSON File

```bash
curl -X GET "https://imt.ryanwallace.cloud/shapes.json" \
  -H "Accept: application/json"
```

## AI Alert Summarization

### Test AI Summarization

```bash
curl -X GET "https://imt.ryanwallace.cloud/ai/test"
```

### Get AI Summarizer Status

```bash
curl -X GET "https://imt.ryanwallace.cloud/ai/status"
```

**Response:**

```json
{
  "enabled": true,
  "model": "llama3.2",
  "base_url": "http://localhost:11434",
  "timeout": 30,
  "temperature": 0.1,
  "queue_status": {
    "pending_jobs": 2,
    "running_jobs": 1,
    "completed_jobs": 15,
    "max_queue_size": 100,
    "max_concurrent_jobs": 3
  }
}
```

### Get AI Summaries for Active Alerts

```bash
curl -X GET "https://imt.ryanwallace.cloud/ai/summaries/active"
```

**Response:**

```json
{
  "status": "success",
  "message": "Generated AI summaries for 5 active alerts",
  "alert_count": 5,
  "model_used": "llama3.2",
  "generated_at": "2024-01-15T10:30:00.123456",
  "summaries": [
    {
      "alert_id": "alert-123",
      "alert_header": "Red Line Delays",
      "alert_short_header": "Red Line Delays",
      "alert_effect": "DELAY",
      "alert_severity": 3,
      "alert_cause": "MAINTENANCE",
      "alert_timeframe": "Ongoing",
      "ai_summary": "Red Line experiencing delays due to maintenance work. Expect 10-15 minute delays.",
      "model_used": "llama3.2",
      "generated_at": "2024-01-15T10:30:00.123456"
    }
  ]
}
```

### Get Cached AI Summaries

```bash
curl -X GET "https://imt.ryanwallace.cloud/ai/summaries/cached"
```

### Get AI Summary for Specific Alert

```bash
curl -X GET "https://imt.ryanwallace.cloud/ai/summaries/alert/alert-123"
```

### Generate AI Summaries for Multiple Alerts

```bash
curl -X POST "https://imt.ryanwallace.cloud/ai/summaries/bulk" \
  -H "Content-Type: application/json" \
  -d '["alert-123", "alert-456", "alert-789"]'
```

## AI Summarization (Legacy Endpoints)

### Summarize Alerts with AI

```bash
curl -X POST "https://imt.ryanwallace.cloud/alerts/summarize" \
  -H "Content-Type: application/json" \
  -d '{
    "alerts": [],
    "config": {
      "max_length": 300,
      "include_route_info": true,
      "include_severity": true
    }
  }'
```

### Check AI Summarizer Health

```bash
curl -X GET "https://imt.ryanwallace.cloud/alerts/summarize/health"
```

## Error Handling

### Rate Limit Exceeded

When you exceed the rate limit, you'll receive:

```json
{
  "detail": "Rate limit exceeded: 100 per 1 minute"
}
```

### Server Errors

Common HTTP status codes:

- `400`: Bad Request (invalid parameters)
- `404`: Not Found (endpoint or resource not found)
- `429`: Too Many Requests (rate limit exceeded)
- `500`: Internal Server Error
- `504`: Gateway Timeout (request timed out)

## Examples with Environment Variables

### Set Base URL

```bash
export API_BASE="https://imt.ryanwallace.cloud"

# Health check
curl -X GET "$API_BASE/health"

# Get vehicles
curl -X GET "$API_BASE/vehicles"

# Test AI summarization
curl -X GET "$API_BASE/ai/test"
```

### Save Responses to Files

```bash
# Save vehicle data to file
curl -X GET "$API_BASE/vehicles" -o vehicles.json

# Save alerts to file
curl -X GET "$API_BASE/alerts" -o alerts.json

# Save route shapes to file
curl -X GET "$API_BASE/shapes" -o shapes.json
```

## Monitoring and Debugging

### Check Response Headers

```bash
curl -I "https://imt.ryanwallace.cloud/health"
```

### Verbose Output

```bash
curl -v "https://imt.ryanwallace.cloud/health"
```

### Follow Redirects

```bash
curl -L "https://imt.ryanwallace.cloud/health"
```

## Notes

- **Large Responses**: Some endpoints (vehicles, alerts, shapes) return large responses. Consider using the streaming endpoint for real-time data.
- **Rate Limiting**: Be mindful of rate limits, especially for AI endpoints which have lower limits.
- **Caching**: Responses are cached in Redis. Use the streaming endpoints for real-time updates.
- **AI Summarization**: Requires Ollama to be running locally with a compatible model (e.g., llama3.2).
- **SSE Streaming**: The vehicles stream endpoint uses Server-Sent Events for real-time updates.

## Troubleshooting

### Connection Issues

```bash
# Test basic connectivity
curl -v "https://imt.ryanwallace.cloud/health"

# Check if Ollama is running (for AI endpoints)
curl -X GET "http://localhost:11434/api/tags"
```

### Performance Issues

- Use the streaming endpoints for real-time data
- Consider implementing client-side caching
- Monitor rate limits and implement exponential backoff

### AI Summarization Issues

- Ensure Ollama is running on `localhost:11434`
- Check that a compatible model is available
- Verify the model name in your configuration
