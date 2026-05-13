FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml requirements.txt ./
COPY src ./src
COPY main.py ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e .

CMD ["python", "main.py", "doctor"]

