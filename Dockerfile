FROM python:3.13@sha256:18634e45b29c0dd1a9a3a3d0781f9f8a221fe32ee7a853db01e9120c710ef535 AS main
COPY --from=ghcr.io/astral-sh/uv:0.8.15@sha256:a5727064a0de127bdb7c9d3c1383f3a9ac307d9f2d8a391edc7896c54289ced0 /uv /uvx /bin/

WORKDIR /app
ADD README.md pyproject.toml uv.lock ./

ENV HF_HOME /app/hf

RUN mkdir hf && uv venv && uv sync --frozen --no-cache --no-install-project --no-dev
# Install project with CPU-only ML extras (no NVIDIA CUDA libs)
# Prefer the PyTorch CPU wheel index on Linux to avoid NVIDIA CUDA deps
RUN uv sync --no-dev --extra ml-cpu \
    --index-url https://download.pytorch.org/whl/cpu \
    --extra-index-url https://pypi.org/simple \
  && uv lock --check

ADD . .

CMD ["uv", "run", "inky-mbta-tracker"]
