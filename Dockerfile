FROM eclipse-temurin:17-jre-jammy

ENV JAVA_HOME=/opt/java/openjdk \
    HADOOP_CONF_DIR=/opt/hadoop-conf \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv procps \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv

WORKDIR /app
COPY requirements.txt ./requirements.txt
ENV PATH="/opt/venv/bin:${JAVA_HOME}/bin:${PATH}"
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY . .
EXPOSE 8501
