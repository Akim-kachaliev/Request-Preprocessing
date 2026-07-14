FROM python:3.11-slim

WORKDIR /app

COPY backend ./backend
COPY ml ./ml
COPY main.py requirements.txt .env.example README.md ./

RUN python -m pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "main.py"]
