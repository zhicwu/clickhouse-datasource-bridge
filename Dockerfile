#
# Docker image for ClickHouse DataSource Bridge.
#

# Stage 1 - build
FROM maven:3.6-jdk-8-openj9 as builder

WORKDIR /app
COPY LICENSE pom.xml /app/
COPY misc /app/misc
COPY src /app/src
RUN mvn -Prelease notice:generate && mvn -q clean package

# Base image
FROM zhicwu/java:8-j9

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

# Environment variables
ENV DATASOURCE_BRIDGE_HOME=/app DATASOURCE_BRIDGE_VERSION="0.1.0" \
	DATASOURCE_BRIDGE_ID=1000 DATASOURCE_BRIDGE_USER=datasource-bridge \
	MAVEN_REPO_URL=https://repo1.maven.org/maven2 \
	CLICKHOUSE_DRIVER_VERSION=0.2.3 \
	MSSSQL_DRIVER_VERSION=7.4.1.jre8 MYSQL_DRIVER_VERSION=2.5.3 \
	POSTGRESQL_DRIVER_VERSION=42.2.9 PRESTO_DRIVER_VERSION=0.230

# Labels
LABEL app_name="ClickHouse DataSource Bridge" app_version="$DATASOURCE_BRIDGE_VERSION"

# Install prerequisities
RUN groupadd -r -g $DATASOURCE_BRIDGE_ID $DATASOURCE_BRIDGE_USER \
	&& useradd -r -u $DATASOURCE_BRIDGE_ID -g $DATASOURCE_BRIDGE_ID $DATASOURCE_BRIDGE_USER \
	&& mkdir -p $DATASOURCE_BRIDGE_HOME/drivers \
	&& apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-unauthenticated apache2-utils \
		apt-transport-https curl htop iftop iptraf iputils-ping lsof net-tools tzdata wget \
	&& wget -P $DATASOURCE_BRIDGE_HOME/drivers \
		$MAVEN_REPO_URL/ru/yandex/clickhouse/clickhouse-jdbc/$CLICKHOUSE_DRIVER_VERSION/clickhouse-jdbc-$CLICKHOUSE_DRIVER_VERSION-shaded.jar \
		$MAVEN_REPO_URL/com/microsoft/sqlserver/mssql-jdbc/$MSSSQL_DRIVER_VERSION/mssql-jdbc-$MSSSQL_DRIVER_VERSION.jar \
		$MAVEN_REPO_URL/org/mariadb/jdbc/mariadb-java-client/$MYSQL_DRIVER_VERSION/mariadb-java-client-$MYSQL_DRIVER_VERSION.jar \
		$MAVEN_REPO_URL/org/postgresql/postgresql/$POSTGRESQL_DRIVER_VERSION/postgresql-$POSTGRESQL_DRIVER_VERSION.jar \
		$MAVEN_REPO_URL/com/facebook/presto/presto-jdbc/$PRESTO_DRIVER_VERSION/presto-jdbc-$PRESTO_DRIVER_VERSION.jar \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR $DATASOURCE_BRIDGE_HOME

COPY --from=builder /app/target/clickhouse-datasource-bridge-*.jar clickhouse-datasource-bridge.jar
COPY docker-entrypoint.sh /docker-entrypoint.sh

RUN chmod +x /*.sh

EXPOSE 9019

VOLUME ["$DATASOURCE_BRIDGE_HOME/config", "$DATASOURCE_BRIDGE_HOME/drivers"]

HEALTHCHECK --start-period=5m --interval=30s --timeout=5s \
	CMD curl --connect-timeout 3 --no-keepalive -f http://localhost:9019/ping || exit 1

ENTRYPOINT ["/sbin/my_init", "--", "/docker-entrypoint.sh"]
