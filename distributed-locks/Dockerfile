FROM python:3.9-slim


WORKDIR /pyapp

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 6000

# ENTRYPOINT ["python3", "distributed_lock_api.py"]

# CMD ["1", "100"]

CMD ["python3", "distributed_lock_api.py"]