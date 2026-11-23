FROM python:3.12-slim
RUN pip install --no-cache-dir redis==5.0.8 psycopg2-binary==2.9.9
WORKDIR /app
COPY etapas.py informe.py /app/
CMD ["python", "-u", "etapas.py"]
