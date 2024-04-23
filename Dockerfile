FROM prefecthq/prefect:2.18-python3.11-conda
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org