FROM socrata/java8-focal

EXPOSE 6030
EXPOSE 6031/udp
ENV JMX_PORT 6039
# EXPOSE 6039

RUN apt-get -y update && apt-get -y install jq

ENV SERVER_ROOT /srv/query-coordinator/
# TODO sbt task to copy latest jar into docker/
ENV SERVER_ARTIFACT query-coordinator-assembly.jar
ENV SERVER_CONFIG query-coordinator.conf

# defaults
ENV ENABLE_GRAPHITE false
ENV GRAPHITE_HOST 0.0.0.0
ENV GRAPHITE_PORT 0
ENV JAVA_XMX 512m
ENV LOG_METRICS false
ENV QUERY_TIMEOUT 10m
ENV MAX_DB_QUERY_TIMEOUT 10m
ENV JAVA_MAX_METASPACE 64m
ENV JAVA_GC_LOG_BEHAVIOR="-Xloggc:/tmp/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=50M -XX:+TraceClassLoading -XX:+PrintGCCause -XX:+TraceClassUnloading"
ENV DELETE_CHUNK_SIZE 200
ENV MIN_QUERY_TIME 3s

WORKDIR $SERVER_ROOT

COPY ship.d /etc/ship.d
COPY ${SERVER_CONFIG}.j2 $SERVER_ROOT/
COPY $SERVER_ARTIFACT $SERVER_ROOT/
