# Use the Ballerina Swan Lake runtime as the base image
FROM ballerina/ballerina:2201.10.1

# Switch to root to ensure proper permissions
USER root

# Set the working directory inside the container
WORKDIR /app

# Copy the Ballerina project files to the working directory
COPY . .

# Ensure the target directory has proper permissions
RUN mkdir -p /app/target && chmod -R 777 /app/target

# Build the Ballerina project
RUN bal build

# Expose the port for the service (adjust as necessary)
EXPOSE 8083 9092

# Run the Ballerina service
CMD ["bal", "run"]
