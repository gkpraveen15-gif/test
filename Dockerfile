FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the data engineering pipeline code into the container
COPY src/ /app/src/
COPY data/ /app/data/

# Set the default command to run when the container starts
CMD ["python", "-c", "from src.pipeline.extract import extract; extract(); print('Pipeline executed successfully in Docker!')"]
