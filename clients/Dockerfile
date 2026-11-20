FROM python:3.12-slim
WORKDIR /app
COPY common/ common/
COPY clients/ clients/
RUN pip install --no-cache-dir pyzmq
ENV COMPONENT_NAME=client
CMD ["python", "-m", "clients.main"]
