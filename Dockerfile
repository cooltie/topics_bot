# 1) Use an official lightweight Python image
FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    python3-dev \
    libffi-dev

# 2) Set the working directory inside the container
WORKDIR /app

# 3) Copy your requirements (if any) and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4) Copy the rest of your code
COPY . .

# 5) Set environment variables if needed
# ENV VAR_NAME=value

# 6) Expose a port if your app serves on a port, e.g. 8000
EXPOSE 8000

# 7) Default command to run your Python app
CMD ["python", "main.py"]