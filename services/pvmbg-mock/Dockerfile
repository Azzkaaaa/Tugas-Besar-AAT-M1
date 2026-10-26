# Build context: repository root.
#   docker build -f services/pvmbg-mock/Dockerfile .
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PVMBG_HTTP_PORT=8080 \
    PVMBG_STORE_DSN=sqlite:///data/pvmbg.sqlite3

COPY services/pvmbg-mock/app.py services/pvmbg-mock/config.py services/pvmbg-mock/storage.py ./
RUN mkdir -p /app/data && chown -R 10001:10001 /app
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ['PVMBG_HTTP_PORT'] + '/health', timeout=2).read()"
CMD ["python", "app.py"]
