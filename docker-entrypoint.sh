#!/bin/bash

set -e

: ${DATASOURCE_BRIDGE_JVM_OPTS:="-Xmx512m"}

init() {
	local config_file=$DATASOURCE_BRIDGE_HOME/config/server.json
	local log4j_config=$DATASOURCE_BRIDGE_HOME/log4j.properties

	if [ ! -f $config_file ]; then
		mkdir -p $DATASOURCE_BRIDGE_HOME/drivers $DATASOURCE_BRIDGE_HOME/config/{datasources,queries}

		cat <<EOF > $config_file
{
	"server": {
		"serverPort": 9019
	}
}
EOF
	fi

	if [ ! -f $log4j_config ]; then
			cat <<EOF > $log4j_config
log4j.rootLogger=INFO, STDOUT
log4j.logger.ru.yandex.clickhouse=WARN
log4j.appender.STDOUT=org.apache.log4j.ConsoleAppender
log4j.appender.STDOUT.layout=org.apache.log4j.PatternLayout
log4j.appender.STDOUT.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss.sss} [%t] [%-5p] {%c{1}:%L} - %m%n
EOF
	fi
}

if [ $# -eq 0 ]; then
	init

	# now start the server
	exec /sbin/setuser $DATASOURCE_BRIDGE_USER \
		java -Djava.awt.headless=true -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle \
			-Xdump:none -Xdump:tool:events=systhrow+throw,filter=*OutOfMemoryError,exec=/usr/bin/oom_killer \
			-cp ./clickhouse-datasource-bridge.jar:$(echo $(ls drivers/*.jar) | tr ' ' ':'):. \
			-Dlog4j.configuration=$DATASOURCE_BRIDGE_HOME/log4j.properties \
			"${DATASOURCE_BRIDGE_JVM_OPTS}" com.github.clickhouse.bridge.DataSourceBridgeVerticle
else
	exec "$@"
fi
