FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY university_data_platform/ university_data_platform/

EXPOSE 8000

CMD ["uvicorn", "university_data_platform.search.api:app", "--host", "0.0.0.0", "--port", "8000"]
