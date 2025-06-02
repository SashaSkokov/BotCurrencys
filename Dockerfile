FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV API_TOKEN=your_api_token
ENV API_KEY=your_api_key
ENV ADMIN_CHAT_ID=your_admin_id

EXPOSE 8000

CMD ["python", "main.py"]