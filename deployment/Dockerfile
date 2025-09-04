# ---- Stage 1: Builder ----
FROM node:18-bullseye AS builder

# Install Python + build dependencies
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

# Create symlink for python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install Python dependencies (with wheels built here)
RUN pip3 install --no-cache-dir \
    ortools==9.7.2996 \
    numpy \
    setuptools \
    wheel

# Create app directory
WORKDIR /usr/src/app

# Copy only package.json for better caching
COPY package*.json ./

# Install Node.js dependencies (production only)
RUN npm ci --only=production

# Copy the rest of the app
COPY . .

# Make script executable
RUN chmod +x services/or_tools_vrp_solver.py


# ---- Stage 2: Runtime ----
FROM node:18-slim

# Install Python runtime (but no compilers)
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copy node_modules from builder
COPY --from=builder /usr/src/app/node_modules ./node_modules

# Copy built app
COPY --from=builder /usr/src/app ./

# Ensure logs directory exists
RUN mkdir -p logs

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:5001/api/health || exit 1

EXPOSE 5001
CMD ["node", "server.js"]
