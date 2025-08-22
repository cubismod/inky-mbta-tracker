## AI Summarization Endpoints

### Test AI Summarization
```bash
# Test with default text format
curl -X GET "http://localhost:8080/ai/test"

# Test with markdown format
curl -X GET "http://localhost:8080/ai/test?format=markdown"

# Test with JSON format
curl -X GET "http://localhost:8080/ai/test?format=json"
```

### Get AI Summarizer Status
```bash
curl -X GET "http://localhost:8080/ai/status"
```

### Get AI Summaries for Active Alerts
```bash
# Get summaries in text format (default)
curl -X GET "http://localhost:8080/ai/summaries/active"

# Get summaries in markdown format
curl -X GET "http://localhost:8080/ai/summaries/active?format=markdown"

# Get summaries in JSON format
curl -X GET "http://localhost:8080/ai/summaries/active?format=json"
```

### Get Cached AI Summaries
```bash
curl -X GET "http://localhost:8080/ai/summaries/cached"
```

### Get AI Summary for Specific Alert
```bash
# Get summary in text format (default)
curl -X GET "http://localhost:8080/ai/summaries/alert/alert_123"

# Get summary in markdown format
curl -X GET "http://localhost:8080/ai/summaries/alert/alert_123?format=markdown"

# Get summary in JSON format
curl -X GET "http://localhost:8080/ai/summaries/alert/alert_123?format=json"
```

### Generate AI Summaries for Multiple Alerts
```bash
# Generate summaries in text format (default)
curl -X POST "http://localhost:8080/ai/summaries/bulk" \
  -H "Content-Type: application/json" \
  -d '["alert_123", "alert_456", "alert_789"]'

# Generate summaries in markdown format
curl -X POST "http://localhost:8080/ai/summaries/bulk?format=markdown" \
  -H "Content-Type: application/json" \
  -d '["alert_123", "alert_456", "alert_789"]'

# Generate summaries in JSON format
curl -X POST "http://localhost:8080/ai/summaries/bulk?format=json" \
  -H "Content-Type: application/json" \
  -d '["alert_123", "alert_456", "alert_789"]'
```

### Generate Summary with Custom Format
```bash
# Generate markdown summary
curl -X POST "http://localhost:8080/alerts/summarize" \
  -H "Content-Type: application/json" \
  -d '{
    "alerts": [],
    "include_route_info": true,
    "include_severity": true,
    "format": "markdown"
  }'

# Generate JSON summary
curl -X POST "http://localhost:8080/alerts/summarize" \
  -H "Content-Type: application/json" \
  -d '{
    "alerts": [],
    "include_route_info": true,
    "include_severity": true,
    "format": "json"
  }'
```

### Format Options

The AI summarization endpoints support three output formats:

- **`text`** (default): Plain text summaries optimized for readability
- **`markdown`**: Formatted summaries with headers, bold text, and proper spacing
- **`json`**: Structured summaries with metadata for programmatic consumption

### Example Responses

#### Markdown Format
```markdown
## Alert Summary for Red Line Service

**Routes Affected:** Red Line
**Effect:** DELAY
**Severity:** MODERATE

Multiple delays on the Red Line due to signal problems at Park Street Station. 
Expect 10-15 minute delays in both directions during rush hour.

**Timeframe:** Until 6:00 PM today
**Cause:** SIGNAL_PROBLEM
```

#### JSON Format
```json
{
  "summary": "Multiple delays on the Red Line due to signal problems...",
  "formatted_summary": {
    "overview": "Multiple delays on the Red Line due to signal problems at Park Street Station",
    "details": [
      "Expect 10-15 minute delays in both directions during rush hour",
      "Routes affected: Red Line",
      "Effect: DELAY",
      "Severity: MODERATE"
    ],
    "alert_count": 1,
    "routes_affected": ["Red Line"],
    "effects": ["DELAY"]
  }
}
```
