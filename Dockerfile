# Copyright (c) Association of Universities for Research in Astronomy
# Distributed under the terms of the Modified BSD License.

# DATB's HST CAL code build for the pipeline
# FROM astroconda/buildsys-pipeline:HCALDP-atodsat-CAL-rc1
# FROM astroconda/buildsys-pipeline:HCALDP_20200708_CAL
# ENV CSYS_VER caldp_20200708

FROM centos:8

LABEL maintainer="dmd_octarine@stsci.edu" \
      vendor="Space Telescope Science Institute"

ENV MKL_THREADING_LAYER="GNU"

USER root

# RUN yum update  -y

# Install s/w dev tools for fitscut build
RUN yum install -y \
   curl \
   wget \
   rsync \
   time \
   git \
   emacs-nox \
   make \
   gcc \
   gcc-c++ \
   gcc-gfortran \
   libpng-devel \
   libjpeg-devel \
   libcurl-devel \
   tar

#   python3 \
#   python3-devel \

RUN mkdir /grp/crds/cache

# Install caldp pip package from local source
WORKDIR /home/developer
USER developer

RUN mkdir caldp-install
ADD . caldp-install/
RUN cd caldp-install && \
    scripts/caldp-install-all  && \
    cd .. && \
    rm -rf caldp-install
RUN echo "/usr/local/lib" >> /etc/ld.so.conf   &&\
    ldconfig

