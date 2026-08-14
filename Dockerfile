FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /nvr .

FROM alpine:latest
RUN apk add --no-cache ffmpeg tzdata
ENV TZ=Europe/Moscow
COPY --from=builder /nvr /usr/bin/simple-nvr
COPY templates/ /usr/share/simple-nvr/templates/
COPY static/ /usr/share/simple-nvr/static/
EXPOSE 8180
EXPOSE 8181
RUN mkdir -p /config
COPY nvr.docker.yaml /config/nvr.yaml
VOLUME /config
CMD ["simple-nvr", "--config", "/config/nvr.yaml", "--static-dir", "/usr/share/simple-nvr"]
