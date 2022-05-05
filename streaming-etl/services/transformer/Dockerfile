FROM python:3.10-alpine

WORKDIR /app

RUN apk add --no-cache build-base libffi-dev bash

RUN pip install poetry

COPY pyproject.toml .
COPY poetry.lock .

RUN poetry install --no-dev

COPY . .

CMD ["poetry", "run", "python3", "main.py"]
