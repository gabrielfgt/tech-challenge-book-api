#!/bin/bash

poetry install --no-root
poetry run uvicorn api.main:api --port 4000 --reload