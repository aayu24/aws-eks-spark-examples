FROM spark:python3

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/spark/work-dir/scripts

USER root

# Create a working directory
WORKDIR /opt/spark/work-dir

# Copy your requirements.txt file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copying additional app dependencies
COPY ./scripts/*.py ./scripts/
COPY ./config/ ./config/