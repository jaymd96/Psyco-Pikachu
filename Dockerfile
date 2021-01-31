# set base image (host OS)
FROM python:3.9-slim

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY Pipfile* .

RUN pip install pipenv

# install dependencies
RUN pipenv lock --keep-outdated --requirements > requirements.txt
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

COPY private/config.ini ./private/config.ini
# command to run on container start
CMD [ "python", "./run.py" ]