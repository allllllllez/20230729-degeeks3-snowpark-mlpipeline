FROM ubuntu:22.10

###############################################################################
# Argments
###############################################################################

ARG python_version=3.8.16

ARG airflow_version=2.5.0
ARG airflow_python_version=3.8


###############################################################################
# Environments
###############################################################################

# 対処： RuntimeError: Unable to find any timezone configuration 
ENV TZ=UTC


###############################################################################
# Install tools
###############################################################################

USER root
RUN apt update && \
  apt -y upgrade && \
  apt install -y \
    # Python ビルドに必要なパッケージ群とライブラリ
    build-essential zlib1g-dev libbz2-dev \
    # pip is configured with locations that require TLS/SSL, however the ssl module in Python is not available.
    libssl-dev \
    # jupyter-notebook のために必要
    libffi-dev libsqlite3-dev \
    curl wget git && \
  wget "https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tgz" && \
  tar -xzvf "Python-${python_version}.tgz" && \
  mv "Python-${python_version}" "/opt/Python${python_version}" 

WORKDIR "/opt/Python${python_version}/"
# https://docs.python.org/ja/3/using/unix.html
RUN ./configure && \
  make && \
  make altinstall && \
  ldconfig "/opt/Python${python_version}" && \
  ln -s /usr/local/bin/python3.8 /usr/local/bin/python && \
  ln -s /usr/local/bin/pip3.8 /usr/local/bin/pip

RUN pip install --upgrade pip && \
  # Snowpark and others, dbt(snowflake)
  # PyOpenSSL: https://github.com/pyca/pyopenssl/issues/1154
  pip install "snowflake-snowpark-python[pandas]==1.5.1" \
    jupyter==1.0.0 numpy==1.23.4 pyOpenSSL==22.0.0 \
    dbt-snowflake==1.5.2 airflow-dbt==0.4.0 \
    numpy==1.23.4 pandas==1.5.1 xgboost==1.5.0 \
    cachetools==4.2.2 dill==0.3.6 matplotlib==3.6.3 && \
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${airflow_version}/constraints-no-providers-${airflow_python_version}.txt" && \
  pip install "apache-airflow==${airflow_version}" --constraint "${CONSTRAINT_URL}"
