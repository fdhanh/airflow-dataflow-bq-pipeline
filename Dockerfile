FROM apache/airflow:2.10.5-python3.9

# Set Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Copy file .env
COPY .env .env

# Upgrade pip
RUN pip install --upgrade pip

# Copy dan install dependencies
COPY requirements.txt $AIRFLOW_HOME/requirements.txt
RUN pip install --no-cache-dir -r $AIRFLOW_HOME/requirements.txt

COPY service-account-hafidah.json service-account-hafidah.json
ENV GOOGLE_APPLICATION_CREDENTIALS=$AIRFLOW_HOME/service-account-hafidah.json