# Copyright (c) Association of Universities for Research in Astronomy
# Distributed under the terms of the Modified BSD License.

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

RUN mkdir -p /grp/crds/cache

# Install caldp pip package from local source
RUN useradd  --user-group --create-home  developer
WORKDIR /home/developer

# Set up scripts directory for conda, fitscut, hstcal installs
RUN mkdir /home/developer/scripts
ADD ./scripts /home/developer/scripts/
RUN chown -R  developer.developer   /home/developer

USER developer

# Install basic conda
RUN mkdir scripts.tmp && cd scripts.tmp && \
   $HOME/scripts/caldp-install-conda && \
   cd $HOME && rm -rf scripts.tmp

# Install fitscut w/o conda or pip packaging
RUN mkdir scripts.tmp && cd scripts.tmp && source $HOME/.bashrc && \
   $HOME/scripts/caldp-install-fitscut && \
   cd $HOME && rm -rf scripts.tmp

# Create caldp_xxxx environment and install HST CAL programs + dependencies
RUN mkdir scripts.tmp && cd scripts.tmp && source $HOME/.bashrc && \
   $HOME/scripts/caldp-install-cal  && \
   cd $HOME && rm -rf scripts.tmp

RUN echo "conda activate caldp_stable" >> $HOME/.bashrc

# Install caldp package from local source code
RUN mkdir /home/developer/caldp
ADD .  /home/developer/caldp
USER root
RUN chown -R developer.developer /home/developer
USER developer
RUN  source ~/.bashrc && cd caldp  && pip install .[test,dev] && \
   cd $HOME && rm -rf caldp

# RUN echo "/usr/local/lib" >> /etc/ld.so.conf   &&\
#    ldconfig
