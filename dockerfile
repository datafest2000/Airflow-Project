FROM apache/airflow:2.8.4

# copy requirements.txt to container

COPY requirements.txt /requirements.txt

# install packages
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt