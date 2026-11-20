FROM python:3.12-slim
WORKDIR /app
COPY common/ common/
COPY services/gt/ services/gt/
RUN pip install --no-cache-dir pyzmq
ENV COMPONENT_NAME=gt
CMD ["python", "-m", "services.gt.main"]
