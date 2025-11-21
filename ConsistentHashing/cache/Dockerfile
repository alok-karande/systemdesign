FROM python:3.9-slim


WORKDIR /pyapp

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 5000

ENTRYPOINT ["python3", "CacheAPIInvocation.py"]

CMD ["1", "100"]