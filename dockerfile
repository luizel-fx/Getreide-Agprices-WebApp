FROM python:3.13

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/list/*


RUN git clone https://github.com/luizel-fx/Getreide-Agprices-WebApp.git

WORKDIR /app/Getreide-Agprices-WebApp

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install streamlit

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]