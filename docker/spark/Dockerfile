FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip docker.io && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m pip install --no-cache-dir \
        numpy \
        pandas \
        kafka-python

# --- Prometheus + Grafana + Supervisord for monitoring ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends supervisor wget libfontconfig1 && \
    rm -rf /var/lib/apt/lists/*

# Download and install Prometheus (multi-arch)
RUN ARCH=$(dpkg --print-architecture) && \
    wget -qO /tmp/prometheus.tar.gz \
      "https://github.com/prometheus/prometheus/releases/download/v2.53.0/prometheus-2.53.0.linux-${ARCH}.tar.gz" && \
    tar xzf /tmp/prometheus.tar.gz -C /tmp && \
    mv /tmp/prometheus-2.53.0.linux-${ARCH}/prometheus /usr/local/bin/ && \
    mv /tmp/prometheus-2.53.0.linux-${ARCH}/promtool /usr/local/bin/ && \
    rm -rf /tmp/prometheus* && \
    mkdir -p /prometheus

# Download and install Grafana (multi-arch)
RUN ARCH=$(dpkg --print-architecture) && \
    wget -qO /tmp/grafana.deb \
      "https://dl.grafana.com/oss/release/grafana_11.1.0_${ARCH}.deb" && \
    apt-get update && \
    dpkg -i /tmp/grafana.deb || apt-get install -f -y && \
    rm /tmp/grafana.deb && \
    rm -rf /var/lib/apt/lists/*

# Create directories for supervisord, Prometheus data, and Grafana dashboards
RUN mkdir -p /var/log/supervisor /var/run /var/lib/grafana/dashboards /etc/prometheus

USER spark