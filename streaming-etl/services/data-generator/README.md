# data-generator

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
