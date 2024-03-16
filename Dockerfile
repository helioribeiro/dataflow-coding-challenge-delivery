FROM python:3.9.2

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "hard.py", "--job_name=hard-challenge-pipeline-docker"]