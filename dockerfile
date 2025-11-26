FROM apache/airflow:2.9.1-python3.10

# 시스템 패키지 설치 (pandas, lxml 빌드용)
USER root
RUN apt-get update && apt-get install -y gcc build-essential

# Python 패키지는 airflow 사용자로 설치해야 함
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt