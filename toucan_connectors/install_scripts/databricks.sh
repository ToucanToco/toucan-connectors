#!/bin/bash
set -e

# The next link was extracted from an email received after
# filling: https://databricks.com/spark/odbc-driver-download
mkdir -p /tmp/databricks
wget 'https://databricks.com/wp-content/uploads/2.6.4.1004/SimbaSparkODBC-2.6.4.1004-Debian-64bit.zip' \
     -o /tmp/databricks/simbaspark.zip
unzip /tmp/databricks/simbaspark.zip -d /tmp/databricks
dpkg -i /tmp/databricks/SimbaSparkODBC-2.6.4.1004-Debian-64bit/simbaspark_2.6.4.1004-2_amd64.deb
rm -rf /tmp/databricks
