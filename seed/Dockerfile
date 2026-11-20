FROM python:3.12-slim
WORKDIR /app
COPY common/ common/
COPY seed/ seed/
RUN pip install --no-cache-dir psycopg[binary] psycopg_pool
ENV COMPONENT_NAME=seed
CMD ["python", "seed/seed.py"]
