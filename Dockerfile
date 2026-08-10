FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /nvr .

FROM alpine:latest
RUN apk add --no-cache ffmpeg tzdata
ENV TZ=Europe/Moscow
COPY --from=builder /nvr /usr/local/bin/nvr
COPY templates/ /app/templates/
COPY static/ /app/static/
EXPOSE 8180
VOLUME /config
WORKDIR /app
CMD ["nvr", "--config", "/config/nvr.yaml"]
