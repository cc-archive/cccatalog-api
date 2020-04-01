FROM python:3.7
RUN pip install pipenv

WORKDIR /worker
COPY . /worker/
RUN pipenv install

CMD YTHONPATH=. pipenv run python worker/consumer.py
