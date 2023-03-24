FROM ubuntu:20.04
RUN apt-get update && apt-get -y install python3 python3-pip libpq-dev
WORKDIR /project/src
COPY ./src .
COPY ./entrypoint.sh /project/src/entrypoint.sh
EXPOSE 5000
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
CMD ["sh", "/project/src/entrypoint.sh"]