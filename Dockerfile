# Copyright (c) Association of Universities for Research in Astronomy
# Distributed under the terms of the Modified BSD License.

# DATB's HST CAL code build for fundamental calibration s/w
ARG CAL_BASE_IMAGE=stsci/hst-pipeline:stable
FROM ${CAL_BASE_IMAGE}

# Keyword added to products
ENV CSYS_VER ${CAL_BASE_IMAGE}

LABEL maintainer="dmd_octarine@stsci.edu" \
      vendor="Space Telescope Science Institute"

# Environment variables
ENV MKL_THREADING_LAYER="GNU"

USER root

# RUN yum update  -y

RUN yum install -y curl rsync time

RUN pip install --upgrade pip
RUN pip install awscli boto3
# RUN pip install jupyterlab
RUN pip install spec-plots==1.34.6

# Install s/w dev tools for fitscut build
RUN yum install -y \
   emacs-nox \
   make \
   gcc \
   gcc-c++ \
   gcc-gfortran \
   python3 \
   python3-devel \
   htop \
   wget \
   git \
   libpng-devel \
   libjpeg-devel \
   libcurl-devel \
   tar

# Install fitscut
COPY scripts/caldp-install-fitscut  .
RUN ./caldp-install-fitscut   /usr/local && \
   rm ./caldp-install-fitscut && \
   echo "/usr/local/lib" >> /etc/ld.so.conf && \
   ldconfig

# Install caldp pip package from local source
RUN mkdir caldp
ADD . caldp/
RUN cd caldp && \
   pip install .[dev,test] && \
   cd .. && rm -rf caldp

RUN mkdir -p /grp/crds/cache
WORKDIR /home/developer
USER developer
