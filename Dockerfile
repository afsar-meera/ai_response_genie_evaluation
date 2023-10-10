FROM ubuntu:20.04

# set timezone
ENV TZ=Asia/Kolkata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN sed -i -e 's/:\/\/(archive.ubuntu.com\|security.ubuntu.com)/old-releases.ubuntu.com/g' /etc/apt/sources.list
RUN apt-get -q update
RUN apt-get -qy install tmux nginx curl git
RUN apt-get install software-properties-common -y
# install python 3.9
RUN add-apt-repository -y ppa:deadsnakes/ppa
RUN apt-get update && apt install -y python3-pip python3.9 python3.9-dev libpq-dev
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 10
RUN apt-get install -y python3.9-distutils
RUN pip3 install --upgrade setuptools && pip3 install --upgrade pip && pip3 install --upgrade distlib

RUN mkdir home/code
RUN mkdir home/data
WORKDIR /home/code

ENV LANG C.UTF-8
ENV LANGUAGE en_US:en
ENV LC_LANG en_US.UTF-8
ENV LC_ALL C.UTF-8
ENV ACCEPT_EULA Y

# install odbc drivers
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN apt-get install -y unixodbc-dev
RUN apt-get install -y msodbcsql17
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN . ~/.bashrc
RUN apt-get autoremove

# create SQL server DSN


COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install "uvicorn[standard]" gunicorn
RUN pip3 install httplib2

COPY . .
# RUN ssh -L 1435:172.16.0.52:1433 -i bastion-linux.pem ec2-user@43.205.69.45
RUN odbcinst -i -s -f ./odbcinst.ini -h
RUN chmod 755 /home/code/auto_run.sh

ENTRYPOINT ["python3", "main.py"]
