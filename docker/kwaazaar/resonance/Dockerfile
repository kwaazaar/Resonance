FROM kwaazaar/mariadb.dotnet

VOLUME /var/lib/mysql
EXPOSE 5000/tcp 3306

CMD /entrypoint.sh

ENV ASPNETCORE_URLS http://*:5000

ENV MARIADB_MAJOR 10.1
ENV MARIADB_VERSION 10.1.18+maria-1~jessie

#ENV MYSQL_ROOT_PASSWORD=Password12! MYSQL_USER=resonance MYSQL_PASSWORD=Password12! MYSQL_DATABASE=resonancedb


ADD entrypoint.sh /entrypoint.sh
RUN sed -i -e 's/\r$//' /entrypoint.sh
RUN chmod 755 /*.sh

COPY ./src /app
WORKDIR /app/Resonance.Core
RUN dotnet restore \
    && dotnet build -c Release -r ubuntu.16.04-x64 -f netstandard1.6
WORKDIR /app/Resonance.Web
RUN dotnet restore \
    && dotnet build -c Release -r ubuntu.16.04-x64 -f netcoreapp1.0
