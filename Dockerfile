FROM ubuntu:20.04

ENV ECCODES_VER=2.23.0
ENV ECCODES_DIR=/opt/eccodes

RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until

RUN apt-get update -y
RUN DEBIAN_FRONTEND="noninteractive" TZ="Europe/Bern" apt-get install -y build-essential curl cmake python3 gfortran python3-pip libffi-dev python3-dev

WORKDIR /tmp/eccodes
RUN curl https://confluence.ecmwf.int/download/attachments/45757960/eccodes-${ECCODES_VER}-Source.tar.gz --output eccodes-${ECCODES_VER}-Source.tar.gz
RUN tar xzf eccodes-${ECCODES_VER}-Source.tar.gz
RUN mkdir build && cd build && cmake -DCMAKE_INSTALL_PREFIX=${ECCODES_DIR} ../eccodes-${ECCODES_VER}-Source && make && ctest && make install
RUN pip3 install eccodes numpy
RUN export PATH="$PATH;/opt/eccodes/bin"
RUN cd / && rm -rf /tmp/eccodes
WORKDIR /
