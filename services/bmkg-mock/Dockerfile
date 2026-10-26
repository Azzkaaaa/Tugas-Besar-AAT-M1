# Build context: repository root.
#   docker build -f services/bmkg-mock/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BMKG_HTTP_PORT=8080 \
    BMKG_STORE_DSN=sqlite:///data/bmkg.sqlite3

WORKDIR /app
COPY services/bmkg-mock/app.py services/bmkg-mock/config.py services/bmkg-mock/storage.py ./
RUN mkdir -p /app/data && chown -R 10001:10001 /app
USER 10001:10001

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ.get('BMKG_HTTP_PORT', '8080') + '/health', timeout=2).close()"

CMD ["python", "app.py"]
