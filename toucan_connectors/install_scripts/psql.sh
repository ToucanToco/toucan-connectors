#!/bin/bash

set -e

if [[ -e ~/odbcdriver-installed ]]; then
    echo "odbc driver already installed"
    exit
fi

apt-get update
apt-get install wget build-essential odbc-postgresql
cd /tmp/
tar xvf unixODBC-2.3.9.tar.gz 
cd unixODBC-2.3.9
./configure
make
make install
export LD_LIBRARY_PATH=/usr/local/lib/

INSTFILE=`odbc_config --odbcinstini`
if [[ -e $INSTFILE ]];
    then
    echo "deleting $INSTFILE "
    rm $INSTFILE
fi

echo "[pgodbc]" >> $INSTFILE;echo "" >> $INSTFILE
echo "Description = ODBC for PostgreSQL" >> $INSTFILE; echo "" >> $INSTFILE;
echo "Driver = /usr/lib/psqlodbcw.so" >> $INSTFILE; echo "" >> $INSTFILE;
echo "Setup = /usr/lib/libodbcpsqlS.so" >> $INSTFILE;  echo "" >> $INSTFILE;
echo "Driver64 = /usr/lib64/psqlodbcw.so" >> $INSTFILE; echo "" >> $INSTFILE;
echo "Setup64 = /usr/lib64/libodbcpsqlS.so" >> $INSTFILE; echo "" >> $INSTFILE;
echo "FileUsage = 1" >> $INSTFILE

if [[ ! -e "/usr/lib/psqlodbcw.so" ]];then
    echo "Driver file doesn't exist copying";
    cp /usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so /usr/lib/psqlodbcw.so;
fi

touch ~/odbcdriver-installed