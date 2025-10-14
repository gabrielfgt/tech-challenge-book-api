#!/bin/bash
# Remove todos os containers, imagens, volumes e redes Docker
# ⚠️ USE COM CUIDADO: isso apaga tudo do Docker local

echo "🧹 Limpando containers..."
docker ps -aq | xargs -r docker rm -f

echo "🧼 Limpando imagens..."
docker images -aq | xargs -r docker rmi -f

echo "🗑️  Limpando volumes..."
docker volume ls -q | xargs -r docker volume rm -f

echo "🌐 Limpando redes..."
docker network ls -q | grep -v "bridge\|host\|none" | xargs -r docker network rm

echo "✅ Limpeza concluída!"