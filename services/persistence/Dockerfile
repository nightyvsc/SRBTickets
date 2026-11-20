FROM python:3.12-slim
WORKDIR /app
COPY common/ common/
COPY services/persistence/ services/persistence/
RUN pip install --no-cache-dir pyzmq psycopg[binary] psycopg_pool
ENV COMPONENT_NAME=persistence
CMD ["python", "-m", "services.persistence.main"]
