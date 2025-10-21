# Use a imagem oficial do Python, mais leve
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip

COPY src ./src

EXPOSE 4000

ARG GIT_HASH=unknown
ENV GIT_HASH=$GIT_HASH

CMD ["opentelemetry-instrument", "uvicorn", "src.main:api", "--host", "0.0.0.0", "--port", "4000"]