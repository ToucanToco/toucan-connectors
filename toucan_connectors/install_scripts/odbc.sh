#!/bin/bash

set -e

if [[ -e ~/odbc-installed ]]; then
    echo "odbc driver already installed"
    exit
fi

apt-get update
apt-get -fyq install wget build-essential odbc-postgresql
cd /tmp/
wget https://public-package.toucantoco.com/connectors_sources/odbc/unixODBC-2.3.9.tar.gz
tar xvf /tmp/unixODBC-2.3.9.tar.gz
cd /tmp/unixODBC-2.3.9
./configure
make
make install
export LD_LIBRARY_PATH=/usr/local/lib/

if [[ ! -e "/usr/lib/psqlodbcw.so" ]];then
    echo "Driver file doesn't exist copying";
    cp /usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so /usr/lib/psqlodbcw.so;
fi

echo "Writing PostgreSQL Driver's config in odbcinst.ini"
echo '[PostgreSQL Unicode]
Description = ODBC Driver for PostgreSQL
Driver = /usr/lib/psqlodbcw.so
Setup = /usr/lib/libodbcpsqlS.so
Driver64 = /usr/lib64/psqlodbcw.so
Setup64 = /usr/lib64/libodbcpsqlS.so
FileUsage = 1' >> /usr/local/etc/odbcinst.ini

touch ~/odbc-installed
