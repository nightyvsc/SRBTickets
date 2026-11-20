FROM python:3.12-slim
WORKDIR /app
COPY common/ common/
COPY services/cancel/ services/cancel/
RUN pip install --no-cache-dir pyzmq
ENV COMPONENT_NAME=cancel
CMD ["python", "-m", "services.cancel.main"]
