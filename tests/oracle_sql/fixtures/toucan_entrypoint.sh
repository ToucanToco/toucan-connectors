#!/bin/bash
# This script will be executed by /entrypoint.sh

# Start oracle on port 1521:
echo "--- Configuring and starting oracle (this can take a while) ---"
printf "8080\\n1521\\noracle\\noracle\\ny\\n" | /etc/init.d/oracle-xe configure

# Load some initial data:
echo "--- Loading /world.sql dataset ---"
echo "exit" | sqlplus system/oracle@localhost:1521 @/world.sql

# Infinite loop to keep the container up and running:
echo "--- Oracle is ready to use. ---"
echo " > username: system"
echo " > password: oracle"
echo " > host:     localhost"
echo " > port:     1521"
echo "--- Hit Ctrl-C to exit. ---"
while [ "$END" == '' ]; do
    sleep 1
    trap "/etc/init.d/oracle-xe stop && END=1" INT TERM
done
