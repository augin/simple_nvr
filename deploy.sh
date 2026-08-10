#!/bin/bash
set -e

SRC=/root/simple_nvr
BIN=/usr/bin/simple-nvr
STATIC=/usr/share/simple-nvr
CONF=/etc/simple-nvr/nvr.yaml

echo "Building..."
cd "$SRC"
export PATH=$PATH:/usr/local/go/bin
CGO_ENABLED=0 go build -ldflags="-s -w" -o nvr .

echo "Deploying..."
cp nvr "$BIN"
chmod 755 "$BIN"

mkdir -p "$STATIC/templates" "$STATIC/static/css" "$STATIC/static/js"
cp templates/index.html "$STATIC/templates/"
cp static/css/style.css "$STATIC/static/css/"
cp static/js/app.js "$STATIC/static/js/"

if [ ! -f "$CONF" ]; then
    mkdir -p /etc/simple-nvr
    cat > "$CONF" <<EOF
base_dir: '/var/lib/simple-nvr/recordings'
archive_dir: '/var/lib/simple-nvr/archive'
stream_server: 'rtsp://127.0.0.1:8554'
target_size_gb: 90
go2rtc_config_path: /etc/go2rtc/go2rtc.yaml
http_port: 8180
EOF
fi

mkdir -p /var/lib/simple-nvr/recordings
mkdir -p /var/lib/simple-nvr/archive

echo "Restarting..."
systemctl daemon-reload
systemctl restart simple-nvr

sleep 1
systemctl status simple-nvr --no-pager | head -5
echo "Done."
