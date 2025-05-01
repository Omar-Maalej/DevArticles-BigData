# Base image with Java, Spark, and Hadoop (for Spark to run properly)
FROM bitnami/spark:latest

# Install Python3 and pip
USER root
RUN apt-get update && apt-get install -y python3 python3-pip

# Set working directory
WORKDIR /app

# Copy all project files
COPY . /app

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1

# Command to run your script 
CMD ["python3", "analyse_articles.py"]
