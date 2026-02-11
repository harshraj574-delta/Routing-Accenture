# deployment/Dockerfile
FROM node:18-bullseye

# Install Python and system dependencies for OR-Tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    gcc \
    g++ \
    cmake \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a symbolic link for python command
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    ortools==9.7.2996 \
    numpy \
    setuptools \
    wheel

# Set Python executable environment variable
ENV PYTHON_EXECUTABLE=python3
ENV PYTHONPATH=/usr/local/lib/python3.9/site-packages

# Create app directory
WORKDIR /usr/src/app

# Copy package files first (for better caching)
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci --only=production

# Copy all application files
COPY . .

# Ensure Python script is executable (correct path)
RUN chmod +x services/or_tools_vrp_solver.py

# Create logs directory
RUN mkdir -p logs

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:5001/api/health || exit 1

# Expose port
EXPOSE 5001

# Start the application
CMD ["node", "server.js"]