# Use official Node.js 20 LTS as the base image
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package.json and package-lock.json (if exists)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy TypeScript configuration and source code
COPY tsconfig.json ./
COPY src ./src

# Copy nodemon configuration for development
COPY nodemon.json ./

# Install nodemon globally for development
RUN npm install -g nodemon

# Compile TypeScript to JavaScript and check for errors
RUN npm run build || { echo "TypeScript compilation failed"; exit 1; }

# Verify dist/index.js exists
RUN test -f dist/index.js || { echo "dist/index.js not found"; exit 1; }

# Expose the application port
EXPOSE 3000

# Command to run the application (overridden in docker-compose for dev/prod)
CMD ["node", "dist/index.js"]