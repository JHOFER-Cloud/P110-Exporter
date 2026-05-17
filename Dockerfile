FROM python:3.14.5-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=9333 \
    MAX_RETRY_COUNT=3

WORKDIR /app

COPY pyproject.toml ./
RUN pip install poetry==1.8.3 && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-interaction --no-ansi

COPY . /app

EXPOSE 9333

CMD ["sh", "-c", "exec python main.py --tapo-email=$TAPO_EMAIL --tapo-password=$TAPO_PASSWORD --config-file=/app/tapo.yaml --prometheus-port=$PORT"]
