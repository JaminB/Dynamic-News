#!/bin/bash
apt-get install python3
apt-get install python3-pip
apt-get install mysql-server
pip3 install pymysql
pip3 install TwitterSearch
mkdir /var/www/dynamic_news/api/
chown www-data /var/www/dynamic_news/api
cp conf/cgi.conf /etc/apache2/sites-available/cgi.conf
cd /etc/apache2/mods-enabled
rm /etc/apache2/sites-enabled/000-default.conf
ln -s ../mods-available/cgi.load .
cd ../sites-enabled
ln -s ../sites-available/cgi.conf
mysql -u root -p < schema.sql
service apache2 restart

