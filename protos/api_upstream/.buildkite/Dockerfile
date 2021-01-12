FROM golang:1.15.6

RUN apt update && apt install -y --no-install-recommends \
      protobuf-compiler \
      && rm -rf /var/lib/apt/lists/* \
# https://github.com/docker-library/golang/blob/c1baf037d71331eb0b8d4c70cff4c29cf124c5e0/1.4/Dockerfile
      && mkdir -p /temporal

WORKDIR /temporal
