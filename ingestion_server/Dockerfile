FROM python:3.7

ENV PYTHONBUFFERED 1

RUN groupadd --system supervisord && useradd --system --gid supervisord supervisord

RUN apt-get update \
    && apt-get install -y supervisor \
    && mkdir -p /var/log/supervisord/ \
    && chown -R supervisord:supervisord /var/log/supervisord

# Install Python dependency management tools
RUN pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install --upgrade pipenv

# Copy all files into the container
COPY . /ingestion_server/
WORKDIR /ingestion_server
RUN chown -R supervisord:supervisord /ingestion_server
ENV PYTHONPATH=$PYTHONPATH:/ingestion_server/

# Install the dependencies system-wide
# TODO: Use build args to avoid installing dev dependencies in production
RUN pipenv install --deploy --system --dev
USER supervisord
EXPOSE 8001
CMD ["supervisord", "-c", "/ingestion_server/config/supervisord.conf"]

# Install the jq tool to filter JSON data 
ENV JQ_VERSION='1.5'

RUN wget --no-check-certificate https://raw.githubusercontent.com/stedolan/jq/master/sig/jq-release.key -O /tmp/jq-release.key && \
    wget --no-check-certificate https://raw.githubusercontent.com/stedolan/jq/master/sig/v${JQ_VERSION}/jq-linux64.asc -O /tmp/jq-linux64.asc && \
    wget --no-check-certificate https://github.com/stedolan/jq/releases/download/jq-${JQ_VERSION}/jq-linux64 -O /tmp/jq-linux64 && \
    gpg --import /tmp/jq-release.key && \
    gpg --verify /tmp/jq-linux64.asc /tmp/jq-linux64 && \
    cp /tmp/jq-linux64 /usr/bin/jq && \
    chmod +x /usr/bin/jq && \
    rm -f /tmp/jq-release.key && \
    rm -f /tmp/jq-linux64.asc && \
    rm -f /tmp/jq-linux64