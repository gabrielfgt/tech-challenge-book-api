
FROM python:3.13-slim

WORKDIR /app

COPY . .
RUN pip install poetry 
RUN poetry install --no-root

EXPOSE 4000

CMD ["./ops_scripts/start_api.sh"]
