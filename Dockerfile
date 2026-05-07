FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg \
  && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY app /app/app
COPY browser-extension /app/browser-extension
COPY scripts/windows /app/scripts/windows

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir .

RUN addgroup --system app && adduser --system --ingroup app app \
  && chown -R app:app /app

USER app

CMD ["python", "-m", "app.main"]
