#!/bin/bash

VOLUME_HOME="/var/lib/mysql"

if [[ ! -d $VOLUME_HOME/resonancedb ]]; then
    echo "Installing Resonance database..."

    mysql_install_db

    /usr/bin/mysqld_safe & sleep 10s

    # mysql -uroot -e 'CREATE USER "root"@"%" IDENTIFIED BY "Password12!"; GRANT ALL ON *.* to "root"@"%";'
    echo "Creating resonancedb..."
    mysql -uroot < '/app/Resonance.Core/content/ResonanceDB.MySql.sql'
    echo "Creating user resonance..."
    mysql -uroot -e 'CREATE USER "resonance"@"%" IDENTIFIED BY "Password12!"; GRANT ALL ON *.* to "resonance"@"%";'
    echo "Stopping mysqld_safe..."
    mysqladmin -uroot shutdown
    echo "=> Done!"  
else
    echo "=> Using an existing database for Resonance"
fi

echo "Starting MariaDB deamon..."

service mysql restart

echo "Starting application..."
dotnet run

