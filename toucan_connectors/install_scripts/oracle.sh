apt-get install -fyq libaio1 curl
mkdir -p /opt/oracle
curl -s 'https://raw.githubusercontent.com/circulosmeos/gdown.pl/master/gdown.pl' -o /tmp/gdown.pl
chmod +x /tmp/gdown.pl
/tmp/gdown.pl 'https://drive.google.com/uc?export=download&id=1prPWRnaVMxDsIiSGJqz0TkFT7wXrCgaO' '/tmp/oracle_client_lib.zip'
unzip /tmp/oracle_client_lib.zip -d /opt/oracle
sh -c "echo /opt/oracle/instantclient_12_2 > /etc/ld.so.conf.d/oracle-instantclient.conf"
ldconfig
rm -rf /tmp/gdown.pl /tmp/oracle_client_lib.zip
