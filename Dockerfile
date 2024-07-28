FROM python:3.9

WORKDIR /pikudHaorefAlertsApp

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH="${PYTHONPATH}:/pikudHaorefAlertsApp/src"

EXPOSE 8000

# Define the command to run the application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]