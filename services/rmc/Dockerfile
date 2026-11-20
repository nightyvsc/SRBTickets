FROM python:3.12-slim
WORKDIR /app
COPY common/ common/
COPY services/rmc/ services/rmc/
RUN pip install --no-cache-dir pyzmq
ENV COMPONENT_NAME=rmc
CMD ["python", "-m", "services.rmc.main"]
