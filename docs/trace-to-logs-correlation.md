# Trace-to-Logs Correlation with Grafana Tempo and Loki

This document explains how to configure and use trace-to-logs correlation between OpenTelemetry traces (stored in Tempo/Jaeger) and logs (stored in Loki) in Grafana.

## Overview

The MBTA Tracker now includes automatic trace context injection into all log records. When OpenTelemetry tracing is active, every log line will include:

- `trace_id` - The unique identifier for the distributed trace
- `span_id` - The identifier for the specific span (operation) within the trace
- `trace_flags` - Trace sampling flags (optional)

This enables bidirectional navigation in Grafana:
- **Trace → Logs**: From a trace span in Tempo, jump directly to all logs emitted during that span
- **Logs → Trace**: From a log line in Loki, jump directly to the trace that generated it

## Implementation

### Log Format Changes

#### JSON Logs (Recommended for Production)

When `IMT_LOG_JSON=true`, logs are output as JSON with trace context:

```json
{
  "time": "2026-01-24T12:34:56.789Z",
  "level": "INFO",
  "logger": "mbta_client",
  "message": "Fetched trip data from cache",
  "file": "mbta_client.py",
  "line": 1005,
  "function": "_get_trip_impl",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7"
}
```

#### Plain Text Logs

When `IMT_LOG_JSON=false`, trace context is added via the `TraceContextFilter` but not displayed in console output (to avoid clutter). The context is still available for structured logging sinks.

### Code Changes

1. **`logging_setup.py`**:
   - Added `TraceContextFilter` class that injects trace context into every log record
   - Updated `JSONFormatter` to include `trace_id` and `span_id` in JSON output
   - Modified `setup_logging()` to automatically apply `TraceContextFilter` to all handlers

2. **Automatic Application**:
   - The filter is applied when `setup_logging()` is called (which happens at application startup)
   - Works for all log handlers: console, file, and custom handlers
   - Zero code changes required in application logic

## Grafana Configuration

### 1. Configure Loki Data Source

In Grafana, go to **Configuration > Data Sources > Loki** and add the following to enable trace-to-logs linking:

**Derived Fields**:

```yaml
- name: TraceID
  regex: '"trace_id":\s*"([0-9a-fA-F]{32})"'
  url: '${__value.raw}'
  datasourceUid: <your-tempo-datasource-uid>
  urlDisplayLabel: View Trace
```

**Alternative regex** (if using plain text logs with trace IDs):
```regex
trace_id=([0-9a-fA-F]{32})
```

### 2. Configure Tempo Data Source

In Grafana, go to **Configuration > Data Sources > Tempo** and configure logs correlation:

**Logs Configuration**:

```yaml
tracesToLogs:
  datasourceUid: <your-loki-datasource-uid>
  spanStartTimeShift: '-1m'
  spanEndTimeShift: '1m'
  filterByTraceID: true
  filterBySpanID: true
  tags:
    - key: service.name
      value: service
```

**Query Builder**:
```
Query: {service_name="inky-mbta-tracker"} | json | trace_id="$${__span.traceId}"
```

### 3. Finding Data Source UIDs

To find your data source UIDs in Grafana:

1. Go to **Configuration > Data Sources**
2. Click on your Loki or Tempo data source
3. Look at the URL - it will contain the UID: `/datasources/edit/<uid>`

Or use the Grafana API:
```bash
curl -H "Authorization: Bearer <your-api-key>" \
  https://your-grafana.example.com/api/datasources
```

## Environment Variables

### Required for Tracing

```bash
# Enable OpenTelemetry tracing
export IMT_OTEL_ENABLED=true

# OTLP endpoint (Tempo or OTEL Collector)
export IMT_OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# Service identification
export IMT_OTEL_SERVICE_NAME=inky-mbta-tracker
export IMT_OTEL_SERVICE_VERSION=1.2.0
export IMT_OTEL_DEPLOYMENT_ENVIRONMENT=production
```

### Required for Log Correlation

```bash
# Enable JSON logging for structured logs with trace context
export IMT_LOG_JSON=true

# Optional: Set log level
export IMT_LOG_LEVEL=INFO
```

## Using Trace-to-Logs Correlation in Grafana

### From Tempo (Trace → Logs)

1. Open **Explore** in Grafana
2. Select **Tempo** as the data source
3. Query for a trace (by trace ID, service, or operation)
4. Click on any span in the trace
5. In the span details panel, click **"Logs for this span"**
6. Grafana will automatically query Loki for all logs with matching `trace_id` and `span_id`

### From Loki (Logs → Trace)

1. Open **Explore** in Grafana
2. Select **Loki** as the data source
3. Query for logs: `{service_name="inky-mbta-tracker"} | json`
4. Find a log line with a `trace_id` field
5. Click the **"Tempo"** or **"View Trace"** link next to the trace_id
6. Grafana will open the corresponding trace in Tempo

## Example Queries

### Loki Query for Traces with Errors

```logql
{service_name="inky-mbta-tracker"} 
| json 
| level="ERROR" 
| line_format "{{.time}} {{.level}} [{{.trace_id}}] {{.message}}"
```

### Loki Query for Logs in a Specific Trace

```logql
{service_name="inky-mbta-tracker"} 
| json 
| trace_id="4bf92f3577b34da6a3ce929d0e0e4736"
```

### Tempo Query with Log Links

```
# Query by service and operation
service.name="inky-mbta-tracker" && name="mbta_client.get_trip"
```

## LogQL Patterns for Common Scenarios

### Find all logs for failed HTTP requests
```logql
{service_name="inky-mbta-tracker"} 
| json 
| http_status_code >= 400
```

### Find logs with specific trace context
```logql
{service_name="inky-mbta-tracker"} 
| json 
| trace_id != "" 
| line_format "{{.time}} [{{.trace_id}}:{{.span_id}}] {{.message}}"
```

### Count errors by trace
```logql
sum by (trace_id) (
  count_over_time(
    {service_name="inky-mbta-tracker"} 
    | json 
    | level="ERROR" [5m]
  )
)
```

## Trace Context Format

### Trace ID Format
- **Length**: 32 hexadecimal characters (128 bits)
- **Example**: `4bf92f3577b34da6a3ce929d0e0e4736`
- **Standard**: OpenTelemetry / W3C Trace Context

### Span ID Format
- **Length**: 16 hexadecimal characters (64 bits)
- **Example**: `00f067aa0ba902b7`
- **Standard**: OpenTelemetry / W3C Trace Context

## Troubleshooting

### Trace IDs not appearing in logs

1. **Check OTEL is enabled**: `IMT_OTEL_ENABLED=true`
2. **Check JSON logging is enabled**: `IMT_LOG_JSON=true`
3. **Verify OTEL initialized**: Look for log message "OpenTelemetry initialized successfully"
4. **Check trace sampling**: Traces may be sampled out (adjust `IMT_OTEL_TRACES_SAMPLER_ARG`)

### Links not working in Grafana

1. **Verify data source UIDs**: Check that Loki and Tempo UIDs are correct in configuration
2. **Check trace ID format**: Ensure regex matches the actual format in logs
3. **Verify time range**: Adjust `spanStartTimeShift` and `spanEndTimeShift` if logs are outside trace time range
4. **Check Loki labels**: Ensure logs have the correct `service_name` label

### Empty log results when clicking from Tempo

1. **Check Loki ingestion**: Verify logs are actually in Loki with correct labels
2. **Verify trace ID format**: Ensure Loki query uses correct JSON path: `trace_id`
3. **Check time range**: Logs might be outside the query time window
4. **Test manual query**: Try manually querying Loki with a known trace_id

## Performance Considerations

### Log Volume Impact

Adding trace context to logs has minimal overhead:
- **Storage**: ~50 bytes per log line (trace_id + span_id fields)
- **CPU**: Negligible (simple context extraction)
- **Network**: Minimal increase in log volume (~5-10%)

### Sampling Strategy

To reduce trace volume while maintaining correlation:

```bash
# Sample 10% of traces (recommended for production)
export IMT_OTEL_TRACES_SAMPLER_ARG=0.1

# High-volume operations (Redis, HTTP clients) sample at 10%
export IMT_OTEL_HIGH_VOLUME_SAMPLE_RATE=0.1

# Background operations sample at 1%
export IMT_OTEL_BACKGROUND_SAMPLE_RATE=0.01
```

**Note**: Even when a trace is not sampled, the trace context is still propagated through logs, allowing you to reconstruct the call chain from logs alone.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Application Code                                    │
│  ├─ OpenTelemetry Instrumentation                   │
│  │  └─ Creates Spans with Trace Context             │
│  └─ Logging                                          │
│     └─ TraceContextFilter injects trace_id/span_id  │
└─────────────────────────────────────────────────────┘
                    │                    │
                    │                    │
         ┌──────────▼─────────┐    ┌────▼──────────┐
         │  OTLP Exporter     │    │  Log Output   │
         │  (gRPC)            │    │  (JSON)       │
         └──────────┬─────────┘    └────┬──────────┘
                    │                    │
                    │                    │
         ┌──────────▼─────────┐    ┌────▼──────────┐
         │  Tempo/Jaeger      │    │  Loki         │
         │  (Traces)          │    │  (Logs)       │
         └──────────┬─────────┘    └────┬──────────┘
                    │                    │
                    └────────┬───────────┘
                             │
                    ┌────────▼─────────┐
                    │  Grafana         │
                    │  ├─ Tempo Panel  │
                    │  └─ Loki Panel   │
                    │                  │
                    │  Correlated via  │
                    │  trace_id        │
                    └──────────────────┘
```

## References

- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)
- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [Grafana Tempo Documentation](https://grafana.com/docs/tempo/latest/)
- [Grafana Loki Documentation](https://grafana.com/docs/loki/latest/)
- [Trace-to-Logs Correlation in Grafana](https://grafana.com/docs/grafana/latest/datasources/tempo/#trace-to-logs)
