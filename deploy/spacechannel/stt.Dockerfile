# [sc] Standalone STT container (upstream runs this natively on macOS).
FROM python:3.12-slim
WORKDIR /app
COPY stt-service/requirements.lock ./requirements.lock
RUN pip install --no-cache-dir -r requirements.lock
COPY stt-service/server.py ./server.py
EXPOSE 8200
CMD ["python", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8200"]
