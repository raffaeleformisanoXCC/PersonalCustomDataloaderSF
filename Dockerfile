FROM python:3.10-bullseye

ADD . /home/

COPY file_env.txt /home/

COPY requirements.txt /home/

USER root

RUN chmod -R 777 /home/

WORKDIR /home/

COPY file_env.txt .env

RUN pip3 install -r requirements.txt

CMD ["tail","-f","/dev/null"]