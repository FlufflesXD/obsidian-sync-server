FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy source
COPY index.js ./

# Expose configurable port (default 8080)
EXPOSE 8080

# Start server
CMD ["node", "index.js"]
