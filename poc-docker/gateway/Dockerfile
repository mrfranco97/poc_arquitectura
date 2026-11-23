FROM python:3.12-slim
RUN pip install --no-cache-dir redis==5.0.8
COPY gateway.py /app/gateway.py
CMD ["python", "-u", "/app/gateway.py"]
