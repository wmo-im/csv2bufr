FROM wmoim/dim_eccodes_baseimage:jammy-2.36.0

# update image
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y wget

# install csv2bufr
WORKDIR /tmp
COPY . /tmp/csv2bufr
RUN cd /tmp/csv2bufr && python3 setup.py install && cd /tmp && rm -R csv2bufr

# install csv2bufr templates
RUN mkdir /opt/csv2bufr &&  \
    cd /opt/csv2bufr && \
    wget https://github.com/wmo-im/csv2bufr-templates/archive/refs/tags/v0.2.tar.gz && \
    tar -zxf v0.2.tar.gz --strip-components=1 csv2bufr-templates-0.2/templates