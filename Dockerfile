# Use the official Python image from the Docker Hub
FROM python:3.9-slim-bookworm

# Set the working directory to /app in the container
WORKDIR /app 

# Copy the current directory contents into the container at /app
COPY /app .

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt 

# Run the command to start the ETL pipeline
CMD ["python", "-m", "etl_project.pipelines.spotify"]