# Use a lightweight node image
FROM node:18-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json (if available)
COPY package*.json ./

# Install dependencies
RUN npm install

# Install nodemon globally to auto-restart the app
RUN npm install -g nodemon

# Copy the rest of the application files
COPY . .

# Run TypeScript build (if you're using TypeScript)
RUN npm run build

# Expose the application port
EXPOSE 3000

# Use nodemon to start the application (in development mode)
CMD ["nodemon", "dist/index.js"]
