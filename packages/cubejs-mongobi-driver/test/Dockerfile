FROM ubuntu:20.04
WORKDIR /home/mongobi

ARG MONGOBI=mongodb-bi-linux-x86_64-ubuntu2004-v2.14.3

RUN apt update \
    && apt install -y libssl-dev libgssapi-krb5-2 wget \
    && wget -qO- https://info-mongodb-com.s3.amazonaws.com/mongodb-bi/v2/${MONGOBI}.tgz | tar -xvz -C /home/mongobi \
    && install -m755 ${MONGOBI}/bin/mongo* /usr/local/bin/ \
    && mkdir /logs

COPY config.yaml config.yaml

EXPOSE 3307
CMD ["mongosqld", "--config=config.yaml"]
