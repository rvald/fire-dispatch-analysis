# Use Python 3.11 as the base image
FROM python:3.11-slim

# Install GCC build dependency
RUN apt-get update && \
    apt-get install -y gcc g++

# Set the working directory in the Docker container
WORKDIR /app

# Copy the project files into the Docker container
COPY . .

# Install the project dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt dependencies
RUN dbt deps 

# Set environment variables for dbt
ENV DBT_PROFILES_DIR=/app

# Set the command to run when the Docker container starts
CMD ["dbt", "run"]