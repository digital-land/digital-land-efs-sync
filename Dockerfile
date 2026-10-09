FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y curl sqlite python3-pip unzip git tcl\
    && apt-get clean

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip \
    && ./aws/install --update

ADD . /src
WORKDIR /src

RUN pip install --user -U pip
RUN pip install --user --no-cache-dir -r requirements/requirements.txt
RUN make dbhash
ENV PATH="$PATH:./bin/sqlite"

RUN chmod +x load.sh retire.sh

# CMD rather than ENTRYPOINT, so Airflow can run ./retire.sh instead. The S3 trigger only sets
# environment variables, so it still runs ./load.sh
CMD [ "./load.sh" ]
