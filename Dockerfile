# Copyright (c) Association of Universities for Research in Astronomy
# Distributed under the terms of the Modified BSD License.

# DATB's HST CAL code build for fundamental calibration s/w
ARG CAL_BASE_IMAGE=stsci/hst-pipeline:stable
FROM ${CAL_BASE_IMAGE}

# Keyword added to products
ENV CSYS_VER=${CAL_BASE_IMAGE}

LABEL vendor="Space Telescope Science Institute"

# Environment variables
ENV MKL_THREADING_LAYER="GNU"

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# ------------------------------------------------------------------------
# SSL/TLS cert setup for STScI AWS firewalling

USER root


# Removing kernel-headers seems to remove glibc and all packages which use them
# Install s/w dev tools for fitscut build
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates \
      emacs-nox \
      make \
      gcc \
      g++ \
      gfortran \
      python3 \
      python3-dev \
      htop \
      wget \
      git \
      libpng-dev \
      libjpeg-dev \
      libcurl4-openssl-dev \
      tar \
      patch \
      curl \
      rsync \
      time \
    && rm -rf /var/lib/apt/lists/*


RUN mkdir -p /usr/local/share/ca-certificates
COPY STSCICA.crt /usr/local/share/ca-certificates/STSCICA.crt
RUN update-ca-certificates

COPY scripts/fix-certs .
RUN ./fix-certs

# Install fitscut
COPY scripts/caldp-install-fitscut  .
RUN ./caldp-install-fitscut   /usr/local && \
   rm ./caldp-install-fitscut && \
   echo "/usr/local/lib" >> /etc/ld.so.conf && \
   ldconfig

# Install caldp pip package from local source
WORKDIR /home/developer
RUN mkdir /home/developer/caldp
COPY . /home/developer/caldp/
RUN chown -R developer.developer /home/developer

# CRDS cache mount point or container storage.
RUN mkdir -p /grp/crds/cache && chown -R developer.developer /grp/crds/cache

# Part #2 of turning on core dumps
# # These are also Terraformed as Docker --ulimit,  container runtime also needs to permit.
# RUN echo "*               soft    core            -1" >> /etc/security/limits.conf &&\
#     echo "*               hard    core            -1" >> /etc/security/limits.conf


# ------------------------------------------------
USER developer
# for any base docker image created later than and including stsci/hst-pipeline:CALDP_20220420_CAL_final,
# the critical base environment is now buried in a conda environment named "linux"
# this creates various issues with the docker run command
# I played around for several hours with ways to bury the conda activation in a .bashrc or .bash_profile,
# but I couldn't get docker to use it when the image was invoked with the docker run command.
# in the end, the least-risky way to fix the issue seems to be to hardcode the conda activation into the path
# and bake it straight into the image.
# --bhayden, 5-24-22
ENV PATH=/opt/conda/envs/linux/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN cd caldp  && \
    pip install .[dev,test]
