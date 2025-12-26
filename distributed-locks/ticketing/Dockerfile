FROM python:3.9-slim


WORKDIR /pyapp

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 7000

ENTRYPOINT [ "python3", "ticketing_service_api.py" ]

# Here we pass three arguments: 
# The first is the host for the Postgresql DB (host.docker.internal to connect to host machine from Docker container))
# We dont pass port here as it's hardcoded in the code to 5555
# The second is the distributed lock service URL with Port 
# The third is the ephemeral lock service URL with Port
CMD ["host.docker.internal", "http://host.docker.internal:6000", "http://host.docker.internal:6001"]

# CMD ["python3", "ticketing_service_api.py"]