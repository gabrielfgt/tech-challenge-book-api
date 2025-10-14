# tech-challenge-book-api
Projeto desenvolvido para o Tech Challenge de criação de uma API pública para consulta de livros, com dados obtidos via web scraping

# Start Project (without docker compose)

## 1 - Start virtual env
```bash
python -m venv .venv
```

## 2 - Activate venv (linux)

### - Linux
```bash
source ./.venv/bin/activate
```

### - Windows
```powershell
.\.venv\Scripts\Activate.ps1
```

## 3 - Install dependencies
```bash
pip install -r requirements.txt
```

## 4 - Create the `.env` file
```md
JWT_SECRET=example_secret
USE_DATABASE=False
```

## 5 - Run project
### - Linux
```bash
./devops/start_local.sh
```
### - Windows
```powershell
.\devops\start_local.ps1
```

# Running the project with **Docker Compose**
## 1 - Create and fill the `.env` file
```md
JWT_SECRET=example_secret
USE_DATABASE=False
DB_HOST=postgres
DB_USER=admin
DB_PASSWORD=admin
```
## 2 - Start Docker Compose

```bash
docker compose up
```

# App components:
- Api: Running on `http://localhost:4000`
- Postgres: Running on `localhost:5432`
- PgAdmin4: Running on `http://localhost:5050`
  - login: `admin@admin.com`
  - password: `admin`
  - connection:
    - host: `postgres`
    - username: `admin`
    - password: `admin`