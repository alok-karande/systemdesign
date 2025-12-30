FROM python:3.9-slim


WORKDIR /pyapp

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 6001


CMD ["python3", "ephemeral_node_api.py"]