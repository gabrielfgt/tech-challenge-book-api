# Remove variáveis de ambiente antigas
Remove-Item Env:JWT_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:USE_DATABASE -ErrorAction SilentlyContinue
Remove-Item Env:DB_HOST -ErrorAction SilentlyContinue
Remove-Item Env:DB_USER -ErrorAction SilentlyContinue
Remove-Item Env:DB_PASSWORD -ErrorAction SilentlyContinue

# Define novas variáveis de ambiente
$Env:JWT_SECRET = "secret_here"
$Env:USE_DATABASE = "False"

# Executa o servidor FastAPI
uvicorn src.app:api --host 0.0.0.0 --port 4000 --reload