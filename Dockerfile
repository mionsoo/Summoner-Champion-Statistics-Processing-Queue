# ------------------------------------------------------------------------------
# Base image
# ------------------------------------------------------------------------------
FROM python:3.11-slim

# ------------------------------------------------------------------------------
# Install dependencies
# ------------------------------------------------------------------------------
FROM python:3.11-slim AS deps

# RUN apt-get update && apt-get install -y git && apt-get install -y python3-h5py
RUN apt-get update && apt-get install -y git


COPY requirements.txt ./


RUN apt update > /dev/null && \
        pip install --disable-pip-version-check -r requirements.txt

# ------------------------------------------------------------------------------
# Final image
# ------------------------------------------------------------------------------
FROM python:3.11-slim
WORKDIR /usr/src/app
COPY . /usr/src/app

COPY --from=deps /root/.cache /root/.cache
RUN pip install --disable-pip-version-check -r requirements.txt && \
        rm -rf /root/.cache



