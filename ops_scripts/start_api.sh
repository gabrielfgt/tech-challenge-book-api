#!/bin/bash

poetry run uvicorn api.main:api --host 0.0.0.0 --port 4000