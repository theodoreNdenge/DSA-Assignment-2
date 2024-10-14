# Use Ubuntu as the base image to install system dependencies
FROM ubuntu:20.04 as build

# Install necessary system dependencies (e.g., curl, git)
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && apt-get clean

# Now, use the Ballerina runtime image
FROM ballerina/ballerina:2201.10.1

# Switch to root user to modify directories without permission issues
USER root

# Set the working directory inside the container
WORKDIR /app

# Ensure the permissions of the directory are correct (including /app/target)
RUN mkdir -p /app && chmod -R 777 /app

# Copy the Ballerina project files to the working directory
COPY . .

# Create the target directory with proper permissions and build the project
RUN mkdir -p /app/target && chmod -R 777 /app/target && bal build

# Expose the port that the service will be running on (adjust as necessary)
EXPOSE 8080 9092

# Run the Ballerina service
CMD ["bal", "run"]
