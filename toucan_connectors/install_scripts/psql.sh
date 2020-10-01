#!/bin/bash

set -e

if [[ -e ~/odbcdriver-installed ]]; then
    echo "odbc driver already installed"
    exit
fi

apt-get update
apt-get install wget build-essential odbc-postgresql
cd /tmp/
wget http://www.unixodbc.org/unixODBC-2.3.9.tar.gz
tar xvf /tmp/unixODBC-2.3.9.tar.gz
cd /tmp/unixODBC-2.3.9
./configure
make
make install
export LD_LIBRARY_PATH=/usr/local/lib/
touch ~/odbcdriver-installed