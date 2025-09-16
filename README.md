# tech-challenge-book-api
Projeto desenvolvido para o Tech Challenge de criação de uma API pública para consulta de livros, com dados obtidos via web scraping

## Setup Development Environment
- Install [Python](https://www.python.org/downloads/release/python-3120/)
- Install poetry
```shell
pip install poetry
```
- Run the configuration below:
```shell
 poetry config virtualenvs.in-project true
```

## Start Local

- Windows:
```powershell
poetry install --no-root
poetry run uvicorn api.main:api --port 4000 --reload
```

- Linux
```shell
./ops_scripts/start_local.sh
```
