# Environment Variables

This document describes all the environment variables that can be used to configure the MBTA tracker application.

## Ollama AI Configuration

The following environment variables control the AI summarization feature. These take **highest priority** over `config.json` settings.

### Required Environment Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `OLLAMA_BASE_URL` | Ollama API endpoint URL | `http://localhost:11434` | `http://ollama:11434` |
| `OLLAMA_MODEL` | AI model to use for summarization | `llama3.2:1b` | `gemma3:1b` |

### Optional Environment Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `OLLAMA_TIMEOUT` | Request timeout in seconds | `30` | `60` |
| `OLLAMA_TEMPERATURE` | Model temperature (0.0-1.0) | `0.1` | `0.2` |
| `OLLAMA_MAX_RETRIES` | Maximum retry attempts for API calls | `3` | `5` |
| `OLLAMA_CACHE_TTL` | Cache TTL in seconds | `3600` | `1800` |

## Configuration Priority

The application loads configuration in this order (highest to lowest priority):

1. **Environment Variables** - Set these for deployment-specific configuration
2. **config.json** - Fallback for local development
3. **Hardcoded Defaults** - Final fallback if nothing else is available

## Usage Examples

### Docker Compose

```yaml
version: '3.8'
services:
  mbta-tracker:
    environment:
      - OLLAMA_BASE_URL=http://ollama:11434
      - OLLAMA_MODEL=gemma3:1b
      - OLLAMA_TIMEOUT=60
      - OLLAMA_TEMPERATURE=0.2
      - OLLAMA_MAX_RETRIES=5
      - OLLAMA_CACHE_TTL=1800
```

### Docker Run

```bash
docker run -e OLLAMA_BASE_URL=http://ollama:11434 \
           -e OLLAMA_MODEL=gemma3:1b \
           -e OLLAMA_TIMEOUT=60 \
           mbta-tracker
```

### Local Development

```bash
export OLLAMA_BASE_URL=http://localhost:11434
export OLLAMA_MODEL=llama3.2:1b
export OLLAMA_TEMPERATURE=0.1
python -m inky_mbta_tracker.main
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: mbta-tracker
        env:
        - name: OLLAMA_BASE_URL
          value: "http://ollama-service:11434"
        - name: OLLAMA_MODEL
          value: "gemma3:1b"
        - name: OLLAMA_TIMEOUT
          value: "60"
```

## Benefits of Environment Variables

- **Deployment Flexibility**: Easy to configure different environments
- **Containerization**: Perfect for Docker and Kubernetes deployments
- **Security**: Sensitive configs can be injected via secrets
- **CI/CD**: Easy to override in build pipelines
- **No File Changes**: Configuration without modifying code or config files

## Fallback Behavior

If environment variables are not set:
1. The application will try to load from `config.json`
2. If that fails, it will use hardcoded defaults
3. The application will log which configuration source it's using

## Logging

The application logs which configuration source it's using:

```
INFO     Using environment variables for Ollama config
INFO     Loaded Ollama config from config.json: {...}
INFO     Using hardcoded defaults for Ollama config
```

This helps with debugging configuration issues in different environments.
