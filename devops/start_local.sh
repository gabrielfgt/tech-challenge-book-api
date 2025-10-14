#!/bin/bash

unset JWT_SECRET
unset USE_DATABASE
unset DB_HOST
unset DB_USER
unset DB_PASSWORD

export JWT_SECRET=secret_here
export USE_DATABASE=False
uvicorn src.app:api --host 0.0.0.0 --port 4000 --reload