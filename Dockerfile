# Use a imagem oficial do Python, mais leve
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip

COPY . .

EXPOSE 4000

CMD ["./ops_scripts/start_api.sh"]