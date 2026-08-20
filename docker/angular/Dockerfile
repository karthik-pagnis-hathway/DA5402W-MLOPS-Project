FROM node:22-alpine

# Set working directory
WORKDIR /app

# Copy dependency definitions and install

COPY src/frontend/package*.json /app/
RUN npm ci

# Copy application source code
COPY src/frontend/ /app/

# Expose port 4200
EXPOSE 4200

# Start development server
CMD ["npx", "ng", "serve", "--host", "0.0.0.0", "--port", "4200"]
