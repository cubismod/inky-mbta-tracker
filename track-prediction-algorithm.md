# Track Prediction Algorithm

This document describes how the track prediction algorithm in `inky-mbta-tracker/track_predictor/track_predictor.py` works to predict commuter rail track assignments before they are officially announced by the MBTA.

## Overview

The track prediction system analyzes historical track assignment patterns to predict future track assignments. It uses machine learning techniques combined with domain-specific heuristics to achieve high accuracy predictions.

## Core Components

### 1. Historical Data Storage (`store_historical_assignment`)

The system stores track assignments in Redis with:
- **Primary Key**: `track_history:{station_id}:{route_id}:{trip_id}:{scheduled_date}`
- **Time Series Index**: `track_timeseries:{station_id}:{route_id}` (sorted set by timestamp)
- **Retention**: 1 year for comprehensive historical analysis
- **Deduplication**: Prevents storing duplicate assignments

### 2. Pattern Analysis Engine (`analyze_patterns`)

The core prediction algorithm analyzes historical patterns using multiple weighted criteria:

#### Similarity Metrics
- **Headsign Similarity**: Uses Levenshtein distance to match trip destinations
- **Time Matching**: Compares departure times with configurable windows (10-20 minutes)
- **Direction Matching**: Ensures same direction (inbound/outbound)
- **Service Pattern**: Distinguishes weekday vs weekend vs holiday schedules

#### Scoring System
The algorithm assigns base scores based on match quality:
- **Exact Match** (15.0 points): Headsign + time + direction + service pattern
- **Strong Headsign** (10.0 points): High headsign similarity + direction + service
- **Precise Time** (8.0 points): Direction + tight time window + service pattern
- **Time Window** (5.0 points): Direction + broader time window
- **Service Pattern** (4.0 points): Direction + same day type
- **Holiday Pattern** (6.0 points): Both trips on holidays + direction
- **Day of Week** (2.0 points): Same weekday

#### Enhanced Confidence Calculation
The algorithm combines multiple factors for final confidence scores:
- **Base Confidence** (35%): Raw pattern matching score
- **Sample Confidence** (25%): Logarithmic scaling based on historical data volume
- **Track Accuracy** (20%): Historical accuracy for this specific track
- **Recency Factor** (10%): Recent prediction success rate
- **Consistency Factor** (10%): How consistently the track is used

#### Time Decay and Quality Adjustments
- **Exponential Time Decay**: Recent data weighted more heavily (0.95^(days/7))
- **Data Quality Factor**: Adjusts for missing data, delays, and data completeness
- **Recency Boost**: Recent assignments (≤7 days) get 1.2x multiplier

### 3. Prediction Generation (`predict_track`)

#### Primary Prediction Methods
1. **Platform Code** (confidence: 1.0): Uses official MBTA platform assignments when available
2. **High Confidence Pattern** (≥0.8): Strong historical pattern match
3. **Medium Confidence Pattern** (0.6-0.8): Good pattern match
4. **Low Confidence Pattern** (0.25-0.6): Weak but usable pattern

#### Dynamic Thresholding
- Minimum confidence threshold adapts based on historical accuracy
- Formula: `max(0.25, min(0.5, overall_accuracy * 0.6))`
- Prevents predictions when confidence is too low

#### Caching and Performance
- **Cache TTL**: 30 minutes to balance freshness and performance
- **Rate Limiting**: Integrates with MBTA API rate limits
- **Metrics**: Comprehensive Prometheus metrics for monitoring

### 4. Validation and Learning (`validate_prediction`)

#### Accuracy Tracking
- **Overall Statistics**: Station/route level accuracy rates
- **Per-Track Accuracy**: Track-specific success rates
- **Method Accuracy**: Success rates by prediction method
- **Recent Success Rate**: 30-day rolling accuracy window

#### Feedback Loop
- Real track assignments validate predictions
- Accuracy data feeds back into confidence calculations
- Failed predictions lower confidence for similar patterns
- Successful predictions boost confidence scores

## Algorithm Flow

```
1. Historical Assignment → Redis Storage
2. Prediction Request → Pattern Analysis
3. Pattern Analysis → Confidence Scoring
4. Confidence Scoring → Threshold Check
5. Threshold Check → Prediction Generation
6. Actual Assignment → Validation
7. Validation → Accuracy Update
8. Accuracy Update → Improved Future Predictions
```

## Key Features

### Multi-Factor Analysis
- Combines temporal, spatial, and service pattern matching
- Weighted scoring system balances different signal strengths
- Adaptive thresholds based on historical performance

### Robust Error Handling
- Graceful degradation for missing data
- Connection failure recovery
- Data validation at all stages

### Comprehensive Metrics
- Prediction accuracy by station, route, and track
- Performance monitoring and alerting
- Method effectiveness tracking

### Holiday and Special Service Detection
- Identifies major holidays affecting schedules
- Adjusts patterns for special service days
- Thanksgiving calculation for variable dates

## Performance Characteristics

- **Prediction Latency**: ~100-500ms depending on historical data volume
- **Cache Hit Rate**: ~80-90% for repeated predictions
- **Accuracy Rate**: Typically 70-85% depending on route and station
- **Memory Usage**: Efficient Redis storage with TTL-based cleanup

## Configuration

The system is configured through environment variables:
- `IMT_REDIS_ENDPOINT`: Redis server hostname
- `IMT_REDIS_PORT`: Redis server port (default: 6379)
- `IMT_REDIS_PASSWORD`: Redis authentication password

## Limitations

- Only works for commuter rail (routes starting with "CR")
- Requires sufficient historical data (minimum 5-10 assignments)
- Accuracy varies by station complexity and schedule consistency
- Cannot predict during major service disruptions or construction