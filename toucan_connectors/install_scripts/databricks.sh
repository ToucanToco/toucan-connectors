#!/bin/bash
set -e

if [[ -e ~/databricks-installed ]]; then
    echo "Databricks connector dependencies are already installed."
    exit
fi

apt-get update
apt-get install -fyq libsasl2-modules-gssapi-mit wget unzip
mkdir -p /tmp/databricks
# The next link was extracted from an email received after
# filling: https://databricks.com/spark/odbc-driver-download
wget 'https://public-package.toucantoco.com/connectors_sources/databricks/SimbaSparkODBC-2.6.4.1004-Debian-64bit.zip' \
     -O /tmp/databricks/simbaspark.zip
unzip /tmp/databricks/simbaspark.zip -d /tmp/databricks
dpkg -i /tmp/databricks/SimbaSparkODBC-2.6.4.1004-Debian-64bit/simbaspark_2.6.4.1004-2_amd64.deb
rm -rf /tmp/databricks

echo "Writing Simba Spark Driver's config in odbcinst.ini"
echo '[Simba Spark ODBC Driver]
Description = ODBC Driver for Databricks
Driver = /opt/simba/spark/lib/64/libsparkodbc_sb64.so' >> /usr/local/etc/odbcinst.ini
touch ~/databricks-installed
