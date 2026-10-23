FROM eclipse-temurin:17-jre-jammy

ARG HADOOP_VERSION=3.5.0
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o "/tmp/hadoop-${HADOOP_VERSION}.tar.gz" \
    && curl -fsSL "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512" -o "/tmp/hadoop-${HADOOP_VERSION}.tar.gz.sha512" \
    && (cd /tmp && awk '{ name=$2; gsub(/[()]/, "", name); print $4 "  " name }' "hadoop-${HADOOP_VERSION}.tar.gz.sha512" | sha512sum -c -) \
    && mkdir -p "${HADOOP_HOME}" \
    && tar -xzf "/tmp/hadoop-${HADOOP_VERSION}.tar.gz" --strip-components=1 -C "${HADOOP_HOME}" \
    && rm -f "/tmp/hadoop-${HADOOP_VERSION}.tar.gz" "/tmp/hadoop-${HADOOP_VERSION}.tar.gz.sha512"

COPY conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY conf/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY hdfs-entrypoint.sh /usr/local/bin/hdfs-entrypoint.sh
RUN chmod +x /usr/local/bin/hdfs-entrypoint.sh \
    && mkdir -p /hadoop/dfs/name /hadoop/dfs/data

EXPOSE 9000 9870 9864 9866 9867
ENTRYPOINT ["/usr/local/bin/hdfs-entrypoint.sh"]
