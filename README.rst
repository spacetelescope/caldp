Calibration data pipeline for Hubble Space Telescope Observations
-----------------------------------------------------------------

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge

This repository provides a pipeline for processing observations from the 
Hubble Space Telescope.


License
-------

This project is Copyright (c) STScI and licensed under
the terms of the BSD 3-Clause license. This package is based upon
the `Astropy package template <https://github.com/astropy/package-template>`_
which is licensed under the BSD 3-clause license. See the licenses folder for
more information.


Contributing
------------

We love contributions! caldp is open source,
built on open source, and we'd love to have you hang out in our community.

**Imposter syndrome disclaimer**: We want your help. No, really.

There may be a little voice inside your head that is telling you that you're not
ready to be an open source contributor; that your skills aren't nearly good
enough to contribute. What could you possibly offer a project like this one?

We assure you - the little voice in your head is wrong. If you can write code at
all, you can contribute code to open source. Contributing to open source
projects is a fantastic way to advance one's coding skills. Writing perfect code
isn't the measure of a good developer (that would disqualify all of us!); it's
trying to create something, making mistakes, and learning from those
mistakes. That's how we all improve, and we are happy to help others learn.

Being an open source contributor doesn't just mean writing code, either. You can
help out by writing documentation, tests, or even giving feedback about the
project (and yes - that includes giving feedback about the contribution
process). Some of these contributions may be the most valuable to the project as
a whole, because you're coming to the project with fresh eyes, so you can see
the errors and assumptions that seasoned contributors have glossed over.

Note: This disclaimer was originally written by
`Adrienne Lowe <https://github.com/adriennefriend>`_ for a
`PyCon talk <https://www.youtube.com/watch?v=6Uj746j9Heo>`_, and was adapted by
caldp based on its use in the README file for the
`MetPy project <https://github.com/Unidata/MetPy>`_.


Overview of CALDP
-----------------

CALDP is used to integrate fundamental HST calibration programs (e.g. calacs.e)
with input data,  output data, and calibration reference files (CRDS).  Ultimately,
CALDP does end-to-end calibration of HST data in a manner similar to the
archive pipeline,  including the generation of preview images.

CALDP is primarily a Python package with some installable scripts, but also includes
infrastructure for building a Docker container with everything needed to fully calibrate
raw HST data to produce pipeline-like products.

CALDP has two basic ways it can be run:

1. CALDP can be run a native conda environment.
2. CALDP can be run inside a Docker container.

A variation of running CALDP inside the Docker container is:

3. Run arbitrary numbers of CALDP containers on AWS compute clusters,
pulling inputs from Astroquery, and writing outputs to AWS S3 storage.

Native CALDP
------------

The core logic of CALDP is implemented in the caldp Python package in the
process and preview modules.  CALDP also includes convience scripts to
make it simpler to configure and call these modules,  as well as to work
with the CALDP container.

Native Install
==============
1. Install a base HST Calibration S/W environment,  including CRDS

2. From a CALDP github checkout, do:

.. code-block:: sh

    # Install CALDP natively
    pip install .

    # Install the fitscut program CALDP needs natively
    caldp-install-fitscut


Native Run
==========


The abstract command for running CALDP natively is:

.. code-block:: sh

    caldp-process   <ipppssoot>   [<input_path>]  [<output_path>]   [<config>]

.. csv-table:: **Parameter Definitions**
    :header: "Parameter",  "Default Value", "Description"
    :widths: 15, 15, 50

    ipppssoot, N/A, "HST dataset identifier,  you must always specify this"
    input_path, file:., "can be file:<relative_path> or astroquery: or (probably coming s3://input-bucket/subdirs...)"
    output_path, file:., "can be file:<relative_path> os s3://output-bucket/subdirs..."
    config, caldp-config-onsite, "can be caldp-config-offsite,  caldp-config-onsite,  caldp-config-aws,  <custom>"

Running natively,  file paths for CALDP work normally with the exception that they're
specified using a URI-like notation which begins with **file:**.

.. csv-table:: **Native Input Path Examples**
    :header: "Syntax",  "Description"
    :widths: 15, 65

    file:., "explicit current working directory, relative path '.'"
    file:inputs, "relative path 'inputs' on local file system"
    file:/home/einstein, "absolute path on local file system"
    astroquery:, "use astroquery to download raw input files"
    s3://bucket/prefix/..., "use an AWS s3 bucket and key to obtain inputs"

.. csv-table:: **Native Output Path Examples**
    :header: "Syntax",  "Description"
    :widths: 15, 65

    file:., "explicit current working directory, relative path '.'"
    file:outputs, "relative path 'outputs' on local file system"
    file:/home/einstein, "absolute path on local file system"
    s3://bucket/prefix/..., "use an AWS s3 bucket and key to write outputs"

Docker CALDP
------------
While CALDP is a natively installable Python package,  its roots are as a Docker container
used to perform HST Calibrations on AWS Batch.  CALDP has been further enhanced to run using
inputs and outputs from a local file system rather than cloud resources like Astroquery and
AWS S3 storage.

Docker Build
============
If you want to run CALDP as a container then the equivalent of installing it
is either building or pulling the container.  This section will cover building
your own CALDP image.

1. Edit scripts/caldp-image-config to set your Docker repo and default tag.  Unless
you're ready to push an image,  you can use any name for your respository.   Leave
the default tag set to "latest" until you're familiar with the scripts and ready
to modify or improve them.

2. From a CALDP github checkout, do:

.. code-block:: sh

    # Install CALDP natively to get convenience scripts
    pip install .

    # This script executes docker build to create the image with your configuration
    caldp-image-build latest

3. (optional) When you're ready to share your image with others,  you can:

.. code-block:: sh

    caldp-image-push latest

Docker Run
==========
The following command configures CALDP to run from a container locally.  It has the advantage
that the entire HST calibration environment is included within the container so there are no
other preliminary setup steps.

.. code-block:: sh

    caldp-docker-run-pipeline  <ipppssoot>  [<input_path>]  [<output_path>]   [<caldp_config>]

This should look very similar to the caldp-process command shown in the *Native CALDP* section above
because it is.

After configuring Docker,  caldp-docker-run-pipeline runs *caldp-process* inside the docker container
with the parameters given on the command line.

Note that file: paths will be interpreted inside the Docker container relative to CALDP_HOME on your
native file system which defaults to ".".

**NOTE:** All files visible in the current working directory are mapped into and can be changed by
CALDP in the Docker container.   This is one aspect of Docker which is tricky,  the file system
*inside* a Docker container and your native file system,  while they can be mapped/overlapped,  are
not automatically the same thing.  e.g. By default your directory "." is "/home/developer" inside
the container.   For security reasons

Examples of input_paths used with Docker

.. csv-table:: **Native Input Path Examples**
    :header: "Syntax",  "Description"
    :widths: 15, 65

    file:., "explicit current working directory, relative path '.'"
    file:inputs, "relative path 'inputs' on local file system"
    astroquery:, "use astroquery to download raw input files"
    s3://bucket/prefix/..., "use an AWS s3 bucket and key to obtain inputs"

.. csv-table:: **Native Output Path Examples**
    :header: "Syntax",  "Description"
    :widths: 15, 65

    file:., "explicit current working directory, relative path '.'"
    file:outputs, "relative path 'outputs' on local file system"
    s3://bucket/prefix/..., "use an AWS s3 bucket and key to write outputs"


Notably absent is `file:/home/einstein`,  mapping absolute paths into Docker hasn't been completely
worked out yet and may never be feasible.


