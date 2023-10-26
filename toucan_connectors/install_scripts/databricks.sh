#!/bin/bash
set -e

if [[ -e ~/databricks-installed ]]; then
    echo "Databricks connector dependencies are already installed."
    exit
fi

apt-get update
apt-get install -fyq libsasl2-modules-gssapi-mit wget unzip
mkdir -p /tmp/databricks
# This package was downloaded from https://www.databricks.com/spark/odbc-drivers-download
wget 'https://public-package.toucantoco.com/connectors_sources/databricks/SimbaSparkODBC-2.7.5.1012-Debian-64bit.zip' \
     -O /tmp/databricks/simbaspark.zip
unzip /tmp/databricks/simbaspark.zip -d /tmp/databricks
dpkg -i /tmp/databricks/simbaspark_2.7.5.1012-2_amd64.deb
rm -rf /tmp/databricks
touch ~/databricks-installed
