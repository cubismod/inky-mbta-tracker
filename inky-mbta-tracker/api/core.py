import logging
import os
import random
from queue import Queue

from ai_summarizer import AISummarizer
from config import load_config
from track_predictor.track_predictor import TrackPredictor
from utils import get_redis

# ----------------------------------------------------------------------------
# CONFIGURATION AND GLOBALS
# ----------------------------------------------------------------------------

# This is intended as a separate entrypoint to be run as a separate container
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# Global constants and instances
from vehicles_background_worker import State  # re-export type for Queue

VEHICLES_QUEUE: "Queue[State]" = Queue()
REDIS_CLIENT = get_redis()
CONFIG = load_config()
TRACK_PREDICTOR = TrackPredictor()
AI_SUMMARIZER = AISummarizer(CONFIG.ollama) if CONFIG.ollama.enabled else None

# API timeout/config flags
API_REQUEST_TIMEOUT = int(os.environ.get("IMT_API_REQUEST_TIMEOUT", "30"))  # seconds
TRACK_PREDICTION_TIMEOUT = int(os.environ.get("IMT_TRACK_PREDICTION_TIMEOUT", "15"))
RATE_LIMITING_ENABLED = os.getenv("IMT_RATE_LIMITING_ENABLED", "true").lower() == "true"
SSE_ENABLED = os.getenv("IMT_SSE_ENABLED", "true").lower() == "true"

# Helper used by lifespan for random waits
MINUTE = 60
HOUR = 60 * MINUTE


# Random helper (used in lifespan tasks)
def rand_sleep(min_seconds: int, max_seconds: int) -> int:
    return random.randint(min_seconds, max_seconds)
