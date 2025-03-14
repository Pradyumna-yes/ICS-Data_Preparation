# Step 1: Use an official Python base image
FROM python:3.9-slim

# Step 2: Install ODBC dependencies
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Step 3: Install ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Step 4: Set the working directory inside the container
WORKDIR /app

# Step 5: Upgrade pip
RUN pip install --upgrade pip

# Step 6: Copy the requirements file into the container
COPY requirements.txt .

# Step 7: Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Step 8: Copy the rest of the application code
COPY . .

# Step 9: Create the uploads directory
RUN mkdir -p /app/uploads

# Step 10: Expose the port Flask runs on
EXPOSE 5000

# Step 11: Command to run the Flask app
CMD ["flask", "run", "--host=0.0.0.0"]