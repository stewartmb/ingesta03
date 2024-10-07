# Dockerfile para ingesta desde MongoDB
FROM python:3-slim
WORKDIR /app
COPY ingesta03.py .
RUN pip3 install pymongo boto3
CMD ["python3", "ingesta03.py"]
