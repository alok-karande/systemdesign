
FROM python:3.9-slim

ENV LOCK_TYPE=distributed_lock

WORKDIR /pyapp

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 7000

#CMD [ "python3", "ticketing_service_api.py", "host.docker.internal", "http://host.docker.internal:6000", "http://host.docker.internal:6001", "${LOCK_TYPE}"]
CMD python3 ticketing_service_api.py host.docker.internal http://host.docker.internal:6000 http://host.docker.internal:6001 ${LOCK_TYPE}

# Here we pass three arguments: 
# The first is the host for the Postgresql DB (host.docker.internal to connect to host machine from Docker container))
# We dont pass port here as it's hardcoded in the code to 5555
# The second is the distributed lock service URL with Port 
# The third is the ephemeral lock service URL with Port
# The fourth is the lock type to use: distributed_lock or ephemeral_lock
# CMD ["host.docker.internal", "http://host.docker.internal:6000", "http://host.docker.internal:6001", "${LOCK_TYPE}"]

# CMD ["python3", "ticketing_service_api.py"]