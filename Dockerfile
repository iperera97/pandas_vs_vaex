FROM python:3.8.18-slim

# Install essential system packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-dev \
    python3-pip \
    build-essential \
    unixodbc-dev \
    pkg-config \
    libhdf5-dev \
    libhdf5-serial-dev \
    libhdf5-103 \
    libopenblas-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    cmake \
    git \
    curl \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Upgrade pip and install build tools
RUN pip install --upgrade pip

# Copy requirements and install
COPY requirements.txt .
RUN pip install -r requirements.txt

# # Copy app code
COPY . .

RUN chmod +x run.sh

# Set default command
CMD ["./run.sh"]
