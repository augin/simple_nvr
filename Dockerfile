# syntax=docker/dockerfile:labs

# 0. Prepare images
ARG PYTHON_VERSION="3.11"

FROM python:${PYTHON_VERSION}-alpine AS base

# 2. Collect all files
FROM scratch AS rootfs

COPY --from=/ /opt/simple_nvr/

# 3. Final image
FROM base

# Install ffmpeg, tini (for signal handling),
# and other common tools for the echo source.
# font-droid for FFmpeg drawtext filter (+2MB)
RUN apk add --no-cache tini ffmpeg font-droid

# Hardware Acceleration for Intel CPU (+50MB)
ARG TARGETARCH

COPY --from=rootfs / /

ENTRYPOINT ["/sbin/tini", "--"]
VOLUME /config
WORKDIR /config

CMD ["python3", "dvr.py", "--config_file", "/config/dvr.yaml"]
