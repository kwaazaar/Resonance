#Based on: https://github.com/docker-library/mariadb/tree/92e16504a8e9840eb70937aa2da76f38ea002718/10.1

# vim:set ft=dockerfile:
#FROM debian:jessie
FROM microsoft/dotnet:latest

RUN apt-get update \
	&& apt-get -y install software-properties-common \
	&& apt-get install -y --no-install-recommends apt-transport-https ca-certificates
#	&& apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 0xcbcb082a1bb943db \
#	&& add-apt-repository 'deb [arch=amd64,i386] http://mirrors.supportex.net/mariadb/repo/10.1/debian jessie main' \
#	&& apt-get update

# Key fingerprint = 1993 69E5 404B D5FC 7D2F  E43B CBCB 082A 1BB9 43DB
# MariaDB Package Signing Key <package-signing-key@mariadb.org>
RUN apt-key adv --keyserver ha.pool.sks-keyservers.net --recv-keys 199369E5404BD5FC7D2FE43BCBCB082A1BB943DB \
# pub   1024D/CD2EFD2A 2009-12-15
#       Key fingerprint = 430B DF5C 56E7 C94E 848E  E60C 1C4C BDCD CD2E FD2A
# uid                  Percona MySQL Development Team <mysql-dev@percona.com>
# sub   2048g/2D607DAF 2009-12-15
	&& apt-key adv --keyserver ha.pool.sks-keyservers.net --recv-keys 430BDF5C56E7C94E848EE60C1C4CBDCDCD2EFD2A \
# pub   4096R/8507EFA5 2016-06-30
#       Key fingerprint = 4D1B B29D 63D9 8E42 2B21  13B1 9334 A25F 8507 EFA5
# uid                  Percona MySQL Development Team (Packaging key) <mysql-dev@percona.com>
# sub   4096R/4CAC6D72 2016-06-30
	&& apt-key adv --keyserver ha.pool.sks-keyservers.net --recv-keys 4D1BB29D63D98E422B2113B19334A25F8507EFA5

RUN echo "deb https://repo.percona.com/apt jessie main" > /etc/apt/sources.list.d/percona.list \
	&& { \
		echo 'Package: *'; \
		echo 'Pin: release o=Percona Development Team'; \
		echo 'Pin-Priority: 998'; \
	} > /etc/apt/preferences.d/percona

ENV MARIADB_MAJOR 10.1
ENV MARIADB_VERSION 10.1.18+maria-1~jessie

RUN echo "deb http://ftp.osuosl.org/pub/mariadb/repo/$MARIADB_MAJOR/debian jessie main" > /etc/apt/sources.list.d/mariadb.list \
	&& { \
		echo 'Package: *'; \
		echo 'Pin: release o=MariaDB'; \
		echo 'Pin-Priority: 999'; \
	} > /etc/apt/preferences.d/mariadb
# add repository pinning to make sure dependencies from this MariaDB repo are preferred over Debian dependencies
#  libmariadbclient18 : Depends: libmysqlclient18 (= 5.5.42+maria-1~wheezy) but 5.5.43-0+deb7u1 is to be installed

# the "/var/lib/mysql" stuff here is because the mysql-server postinst doesn't have an explicit way to disable the mysql_install_db codepath besides having a database already "configured" (ie, stuff in /var/lib/mysql/mysql)
# also, we set debconf keys to make APT a little quieter
RUN { \
		echo mariadb-server-$MARIADB_MAJOR mysql-server/root_password password 'Password12!'; \
		echo mariadb-server-$MARIADB_MAJOR mysql-server/root_password_again password 'Password12!'; \
	} | debconf-set-selections

RUN groupadd -r mysql && useradd -r -g mysql mysql \
	&& rm -rf /var/lib/mysql && mkdir -p /var/lib/mysql /var/run/mysqld \
	&& chown -R mysql:mysql /var/lib/mysql /var/run/mysqld \
# ensure that /var/run/mysqld (used for socket and lock files) is writable regardless of the UID our mysqld instance ends up having at runtime
	&& chmod 777 /var/run/mysqld

RUN apt-get update \
	&& apt-get install -y mariadb-server=$MARIADB_VERSION

# make our mysql server listen to more than localhost so that we can connect from outside of our container.
RUN sed -Ei 's/^(bind-address|log)/#&/' /etc/mysql/my.cnf \
	&& echo 'skip-host-cache\nskip-name-resolve' | awk '{ print } $1 == "[mysqld]" && c == 0 { c = 1; system("cat") }' /etc/mysql/my.cnf > /tmp/my.cnf \
	&& mv /tmp/my.cnf /etc/mysql/my.cnf

RUN mysql_install_db

RUN /usr/sbin/mysqld & \
    sleep 10s \
	&& mysql -uroot --password=Password12! -e 'CREATE USER "root"@"%" IDENTIFIED BY "Password12!"; GRANT ALL ON *.* to "root"@"%";' 
	#&& "GRANT ALL ON *.* TO root@'%' IDENTIFIED BY 'Password12!' WITH GRANT OPTION; FLUSH PRIVILEGES" | mysql -u root --password=Password12!
    #&& mysql -u root --password=Password12! -e 'CREATE USER resonance@'"'"'%'"'"' IDENTIFIED BY '"'"'Password12!'"'"'; GRANT ALL ON *.* TO resonance;' \

RUN service mysql restart & sleep 20s

RUN service mysql stop
# Ubuntu
#RUN apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
#RUN add-apt-repository 'deb [arch=amd64,i386,ppc64el] http://mirrors.supportex.net/mariadb/repo/10.1/ubuntu xenial main'

# Debian
#RUN apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 0xcbcb082a1bb943db
#RUN add-apt-repository 'deb [arch=amd64,i386] http://mirrors.supportex.net/mariadb/repo/10.1/debian jessie main'

#RUN apt-get update
#RUN apt-get -y install mariadb-server

VOLUME /var/lib/mysql

EXPOSE 3306

CMD ["mysqld"]

#ENTRYPOINT mysqld

