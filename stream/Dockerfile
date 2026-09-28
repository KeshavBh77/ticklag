FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY windowing.py faust_app.py ./
CMD ["faust", "-A", "faust_app", "worker", "-l", "info"]
