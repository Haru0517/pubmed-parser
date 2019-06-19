FROM python:3.7

LABEL version="1.0"
LABEL maintainer="Yuto Chikazawa <minet6759@gmail.com>"
LABEL description="pubmed XML parser"

ENV PYTHONUNBUFFERED=1

RUN apt-get update -y --fix-missing \
    && apt-get install -y --no-install-recommends \
    git \
    make \
    curl \
    wget \
    vim

WORKDIR /usr/src

ENV PIPENV_VENV_IN_PROJECT=1

COPY ./Pipfile  ./

RUN pip install --upgrade pip \
    && pip install pipenv \
    && pipenv install --skip-lock --system --dev