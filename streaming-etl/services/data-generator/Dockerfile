FROM python:3.8

WORKDIR /app

COPY pyproject.toml .
COPY poetry.lock .

RUN apt update
RUN apt install libpq-dev
RUN pip install poetry
RUN poetry install --no-dev

COPY . .

CMD ["poetry", "run", "python3", "main.py"]
