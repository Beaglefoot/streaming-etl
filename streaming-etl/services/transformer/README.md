# Transformer

## Installation

First of all [install poetry](https://python-poetry.org/docs/#installation).

Then:

```
poetry install
```

## How to start

Either run
```
env $(cat .env.dev) poetry shell
```

and then

```
python main.py
```

or in one command as

```
env $(cat .env.dev) poetry run python main.py
```

## Data models generation

Any change in application DB should be accompanied with re-generation of data models.

There is a helper script for this:

```
./generate_models.sh
```
