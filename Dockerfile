# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies for Oracle client
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Create directories for wallet files
RUN mkdir -p /app/wallet_jsondb /app/wallet_vectdb

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Gunicorn for production
RUN pip install gunicorn

# Copy the rest of the application
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# Command to run the application
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]