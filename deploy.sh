#!/bin/bash
set -e

SRC=/root/simple_nvr
DST=/opt/simple_nvr

echo "Building..."
cd "$SRC"
export PATH=$PATH:/usr/local/go/bin
go build -o nvr .

echo "Deploying..."
cp nvr "$DST/nvr"
cp templates/index.html "$DST/templates/"
cp static/css/style.css "$DST/static/css/"
cp static/js/app.js "$DST/static/js/"

echo "Restarting..."
systemctl restart nvr

sleep 1
systemctl status nvr --no-pager | head -5
echo "Done."
