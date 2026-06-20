FROM caddy:2.11.3-builder-alpine@sha256:52575959b1eeee9900869325a953d71e4c521ab9102dd5cce07d429ea8246b85 AS caddy

RUN xcaddy build \
    --with github.com/mholt/caddy-ratelimit

FROM ghcr.io/cubismod/inky-mbta-tracker:latest AS dinit-builder

RUN apt update && apt install -y build-essential cmake m4 wget && \
    wget -c https://github.com/davmac314/dinit/archive/refs/tags/v0.22.0.tar.gz -O - | tar -xz && \
    cd dinit-0.22.0 && \
    make mconfig && \
    make && \
    make install DESTDIR=/dinit-out && \
    rm -rf /var/lib/apt/lists/*

FROM ghcr.io/cubismod/inky-mbta-tracker:latest

COPY --from=dinit-builder /dinit-out/usr/local/bin/dinit /usr/local/bin/dinit
COPY --from=dinit-builder /dinit-out/usr/local/bin/dinitctl /usr/local/bin/dinitctl

RUN apt update && apt install -y valkey-server && rm -rf /var/lib/apt/lists/*

ADD https://github.com/cloudflare/cloudflared/releases/download/2026.6.1/cloudflared-linux-amd64 /usr/local/bin/cloudflared
RUN chmod +x /usr/local/bin/cloudflared

COPY --from=caddy /usr/bin/caddy /usr/bin/caddy

ADD Caddyfile /etc/caddy/Caddyfile
ADD fly_config.json /app/config.json
