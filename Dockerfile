FROM apache/airflow:3.1.1-python3.12 

USER root
# Install OpenJDK 17 (recommended for RMLMapper)
RUN apt-get update && apt-get install -y openjdk-17-jre && apt-get clean

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow