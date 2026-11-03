FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY data/product_profile.json ./seed/product_profile.json
COPY data/templates ./seed/templates
ENV PYTHONUNBUFFERED=1 AVYUKT_DATA_DIR=/data
# Copy the pilot profile into the (possibly empty) data volume on first start, then serve.
CMD ["sh", "-c", "mkdir -p /data && [ -f /data/product_profile.json ] || cp /app/seed/product_profile.json /data/product_profile.json; python -m src.web"]
