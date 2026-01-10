FROM python:3.10-slim


# Force rebuild to pick up code changes
ENV FORCE_REBUILD=20260110_05

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY extract_crypto.py .

CMD ["python", "extract_crypto.py"]
