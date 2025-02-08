FROM python:3.11 AS runtime

# RUN apt-get update && apt-get install -y jq libhdf5-dev

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 

WORKDIR /

EXPOSE 7100

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./src /src
COPY ./static /static

CMD ["uvicorn", "src.main:app","--host=0.0.0.0", "--port=7100", "--workers=1"]
