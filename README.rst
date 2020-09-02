Calibration data pipeline for Hubble Space Telescope Observations
-----------------------------------------------------------------

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge


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

CALDP is used to integrate fundamental HST calibration programs (*e.g.* calacs.e)
with input data, output data, and calibration reference files (CRDS). Ultimately,

CALDP does end-to-end calibration of HST data in a manner similar to the
archive pipeline, including the generation of preview images.

CALDP is primarily a Python package with some installable scripts, but also includes
infrastructure for building a Docker container with everything needed to fully calibrate
raw HST data to produce pipeline-like products.

CALDP has two basic ways it can be run:

1. CALDP can be run native in a conda environment.
2. CALDP can be run inside a Docker container.

A variation of running CALDP inside the Docker container is:

3. Run arbitrary numbers of CALDP containers on AWS compute clusters, pulling inputs
from Astroquery, and writing outputs to AWS S3 storage. This can vastly accelerate
large processing runs.

Native CALDP
------------

The core logic of CALDP is implemented in the caldp Python package in the
process and create_preview modules.  CALDP also includes convenience scripts to
make it simpler to configure and call these modules.   Since it is primarily
Python,   nothing precludes running CALDP outside a container provided you
install prerequisites.

Native Install
==============

The Everything Install
++++++++++++++++++++++

**WARNING**: By default this install method will completely replace any installation
you already have at $HOME/miniconda3 unlless you supply additional parameters.

The following commands will install:

1. Miniconda
2. The `stable` version of HSTCAL
3. Fitscut
4. Whichever version of CALDP you clone and/or checkout

Parameters specified below in **[ ]** are optional,  but must be specified in order, *i.e.*
to change the CONDA_DIR you must specify all four parameters explicitly.

.. code-block:: sh

    git clone https://github.com/spacetelescope/caldp.git
    cd caldp
    scripts/caldp-install-all   [HSTCAL]  [PY_VER]  [CONDA_ENV]  [CONDA_DIR]

.. csv-table::
    :header: "Parameter",  "Default", "Description"
    :widths: 15, 15, 50

    HSTCAL, stable,"Version of base calibration packages,  nominally *stable* or *latest*."
    PY_VER, 3.6.10,"Python version for CALDP conda environment."
    CONDA_ENV, caldp_stable, "Conda environment which will be created"
    CONDA_DIR, "${HOME}/miniconda3", "Location of Miniconda Installation."


Install Step-by-Step
++++++++++++++++++++

This section breaks down the Everything installation into different functional steps
so that you can omit steps or customize as needed,  *e.g.* if you already have a miniconda3
installation and just want to add to it.

0. Check out the source code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: sh

     git clone https://github.com/spacetelescope/caldp.git
    cd caldp

1. Install base conda environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: sh

    scripts/caldp-install-conda  [CONDA_DIR]
    source ~/.bashrc

2. Install fundamental CAL code using pipeline package lists
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: sh

    scripts/caldp-install-cal  [HSTCAL]  [PY_VER]  [CONDA_ENV]  [CONDA_DIR]
    source $CONDA_DIR/etc/profile.d/conda.sh
    conda activate [CONDA_ENV]

3. Install fitscut for image previews
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: sh

    scripts/caldp-install-fitscut   ${CONDA_DIR}/envs/${CONDA_ENV}

4. Install CALDP and direct dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: sh

    pip install .[dev,test]

While doing CALDP development you can of course just iterate changing, re-installing, and
testing CALDP itself.

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
    output_path, file:., "can be file:<relative_path> or s3://output-bucket/subdirs..."
    config, caldp-config-onsite, "can be caldp-config-offsite,  caldp-config-onsite,  caldp-config-aws,  <custom>"

Running natively, file paths for CALDP work normally with the exception that they're
specified using a URI-like notation which begins with **file:**. Absolute paths work here.

Example Native Commands
+++++++++++++++++++++++
Below are some parameter examples for running CALDP natively with different input
and output modes. caldp-process is configured to run using local files by default.

.. code-block:: sh

    # All file access defaults to current working directory. Inputs must pre-exist.
    # Inputs: Finds raw files matching j8cb010b0 in current working directory
    # Outputs: Puts output product trees under current working directory as data and messages subdirectories.
    # CRDS configuration: VPN configuration, no CRDS server required, /grp/crds/cache must be visible.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-process j8cb010b0

    # ----------------------------------------------------------------------------------------
    # File access in subdirectories, inputs must pre-exist.
    # Inputs: Finds raw files matching j8cb010b0 in subdirectory j8cb010b0_inputs.
    # Outputs: Copies output product tree under subdirectory j8cb010b0_outputs.
    # CRDS configuration: VPN configuration, no CRDS server required, /grp/crds/cache must be visible.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-process j8cb010b0  file:j8cb010b0_inputs  file:j8cb010b0_outputs


    # ----------------------------------------------------------------------------------------
    # Download inputs from astroquery as neeed
    # Inputs: Downloads raw files matching j8cb010b0 from astroquery to current working directory / CALDP_HOME.
    # Outputs: Copies output product tree under subdirectory j8cb010b0_outputs.
    # CRDS configuration: VPN configuration, no CRDS server required, /grp/crds/cache must be visible.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-process j8cb010b0  astroquery:   file:j8cb010b0_outputs


    # ----------------------------------------------------------------------------------------
    # Download inputs from astroquery, upload outputs to S3, current AWS Batch configuration minus Docker.
    # Inputs: Downloads raw files matching j8cb010b0 from astroquery to current working directory / CALDP_HOME.
    # Outputs: Copies output product tree to AWS S3 storage bucket, AWS credentials and permission required.
    # CRDS configuration: VPN configuration, no CRDS server required, /grp/crds/cache must be visible.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-process j8cb010b0  astroquery:  s3://calcloud-hst-pipeline-outputs


Docker CALDP
------------
While CALDP is a natively installable Python package, its roots are as a Docker container
used to perform HST calibrations on AWS Batch. CALDP has subsequently been enhanced to run
using inputs and outputs from a local file system rather than cloud resources like Astroquery
and AWS S3 storage. The primary difference from running natively is that some portion
of your native file system must be mounted inside the container to pass files in and out
as naturally as possible. By default, your current working directory becomes $HOME
(/home/developer)

Docker Build
============
If you want to run CALDP as a container then the equivalent of installing it
is either building or pulling the container (i.e. from an AWS elastic container registry, ECR).
This section will cover building your own CALDP image. To complete this section for
personal use,  all you need is a local installation of Docker and the supplied scripts
should run it for you even more easily than normal. This section doesn't cover using Docker
in general, or hosting your own images on Docker Hub or AWS Elastic Container Registry (ECR)
where you can make them available to others.

0. Clone this repo to a local directory and CD to it.

1. Edit *scripts/caldp-image-config* to set your Docker repo and default tag. Unless
you're ready to push an image, you can use any name for your respository. Leave
the default tag set to "latest" until you're familiar with the scripts and ready
to modify or improve them.

.. code-block:: sh

    git clone https://github.com:/spacetelescope/caldp.git
    cd caldp

2. Configure and build:

    # Edit scripts/caldp-image-config to set the Docker image config variables for
    # your currrent build.  These will include the repo and image tag your want to
    # build and/or push.
    vim scripts/caldp-image-config   # and customize as needed.

    # Install CALDP natively to get convenience scripts and your configuration from (1).
    pip install .

    # This script executes docker build to create the image with your configuration
    caldp-image-build

At this stage you can proceed to running your image if you wish.

3. (optional) When you're ready to share your image with others and have done the corresponding
Docker Hub or ECR setup, you can log in from your shell and then:

.. code-block:: sh

    caldp-image-push

This will push your image to the repo and tag your configured above.

Docker Run
==========
The following command configures CALDP to run from a container locally. It has the advantage
that the entire HST calibration environment is included within the container so there are no
other preliminary setup steps other than setting up Docker. The same container can be run
locally or on pipeline cluster systems like AWS Batch.

.. code-block:: sh

    caldp-docker-run-pipeline  <ipppssoot>  [<input_path>]  [<output_path>]   [<caldp_process_config>]

This should look very similar to the caldp-process command shown in the *Native CALDP* section above
because it is. The primary **differences** are that absolute native paths do not work.

**NOTE:**  The config file specified to caldp-docker-run-pipeline is used to configure processing,
not to select the image.  caldp-docker-run-pipeline automatically uses caldp-image-config to select
the image to run.

Example Docker Commands (Local File System)
+++++++++++++++++++++++++++++++++++++++++++
Below are some parameter examples for running CALDP inside Docker with different input
and output modes. caldp-process is *still* configured to run using local files by default.

.. code-block:: sh

    # All file access defaults to current working directory. Inputs must pre-exist.
    # Inputs: Finds raw files matching j8cb010b0 in current working directory
    # Outputs: Puts output product trees under current working directory as data and messages subdirectories.
    # CRDS configuration: Remote configuration, server https://hst-crds.stsci.edu must be up, files downloaded to crds_cache.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-docker-run-pipeline j8cb010b0

    # ----------------------------------------------------------------------------------------
    # File access in subdirectories, inputs must pre-exist.
    # Inputs: Finds raw files matching j8cb010b0 in subdirectory j8cb010b0_inputs.
    # Outputs: Copies output product tree under subdirectory j8cb010b0_outputs.
    # CRDS configuration: Remote configuration, server https://hst-crds.stsci.edu must be up, files downloaded to crds_cache.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-docker-run-pipeline j8cb010b0  file:j8cb010b0_inputs  file:j8cb010b0_outputs


    # ----------------------------------------------------------------------------------------
    # Download inputs from astroquery as neeed
    # Inputs: Downloads raw files matching j8cb010b0 from astroquery to current working directory / CALDP_HOME.
    # Outputs: Copies output product tree under subdirectory j8cb010b0_outputs.
    # CRDS configuration: Remote configuration, server https://hst-crds.stsci.edu must be up, files downloaded to crds_cache.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-docker-run-pipeline j8cb010b0  astroquery:   file:j8cb010b0_outputs


    # ----------------------------------------------------------------------------------------
    # Download inputs from astroquery, upload outputs to S3, current AWS Batch configuration minus Docker.
    # Inputs: Downloads raw files matching j8cb010b0 from astroquery to current working directory / CALDP_HOME.
    # CRDS configuration: Remote configuration, server https://hst-crds.stsci.edu must be up, files downloaded to crds_cache.
    # Scratch files: Extra processing artifacts appear in the current working directory. Export CALDP_HOME to move them somewhere else.

    caldp-docker-run-pipeline j8cb010b0  astroquery:  s3://calcloud-hst-pipeline-outputs/batch-22

After configuring Docker, caldp-docker-run-pipeline runs *caldp-process* inside the docker container
with the parameters given on the command line. While file: paths are defined relative to your native
file system, within the Docker container they will nominally be interpreted relative to */home/developer*.
Since the CALDP_HOME directory is mounted read/write inside Docker, files needed to process a dataset
will be reflected back out of the Docker container to CALDP_HOME, defaulting to your current working
directory.

**NOTE:**  Running the final cloud-like configuration above does not produce results idenitical to AWS Batch processing
because it is only processing a single dataset and skips batch tracking and organization actions normally performed by
the batch trigger lambda which operates on a list of datasets.

Example Docker Commands (AWS Batch)
+++++++++++++++++++++++++++++++++++
Below is the calling sequence used to run CALDP on AWS Batch. This command is specified in the
AWS Batch job definition and used to run all queued jobs. The calling sequence uses more
customized input parameters in the outermost wrapper script specifying only the S3 output
bucket and dataset name.

.. code-block:: sh

    caldp-process-aws  <s3_output_path>   <ipppssoot>

Internally, *caldp-process-aws* runs *caldp-process* automatically configured to use:

1. astroquery: to obtain raw data.
2. the specified S3 output path which typically includes a batch "subdirectory".
3. the specified dataset (ipppssoot) to define which data to fetch and process.
4. a serverless CRDS configuration dependent only on S3 files.

Despite supporting a containerized use case, since AWS Batch (or equivalent) normally runs
Docker, *caldp-process-aws* is effectively a *native* mode command when run by itself.
There is no wrapper script equivalent to *caldp-docker-run-pipeline* to configure and
run *caldp-process-aws* inside Docker automatically, but since it really requires no additional
file mounts or ports, it is simple to run with Docker.

Running *caldp-process-aws* does require access to the CRDS and the output bucket on AWS S3 storage,
*i.e.* appropriate credentials and permissions.

Debugging in the Container
++++++++++++++++++++++++++
Sometimes you want to execute commands in the container environment rather than *caldp-process*. You
can run any command using *caldp-docker-run-container* which is itself wrapped by *caldp-docker-run-pipeline*.

.. code-block:: sh

    # You can run a shell or other alternate program inside the CALDP container like this:

    caldp-docker-run-container  /bin/bash  # interactive shell at /home/developer inside the container, nominally as user *developer*.

About CALDP_HOME
++++++++++++++++
The CALDP_HOME environment variable defines which native directory *caldp-docker-run-pipeline* will
mount inside the running Docker container at $HOME as read/write. If not exported, CALDP_HOME
defaults to the directory you run caldp-docker-run-pipeline from. Since *caldp-process*
runs at $HOME within the Docker container, any scratch files used during processing will appear
externally within CALDP_HOME. Note that using caldp-docker-run-pipeline is not a requirement,
it is just a script used to establish standard Docker configuration for local CALDP execution.

Getting AWS Credentials Inside the Container
++++++++++++++++++++++++++++++++++++++++++++
One technique for enabling AWS access inside the container is to put a *.aws* configuration directory in your
*CALDP_HOME* directory.

Since caldp-docker-run-pipeline mounts CALDP_HOME inside the container at *$HOME*, AWS will see them where it
expects to find them. AWS Batch nominally runs worker nodes which have the necessary permissions attached
so no .aws directory is needed on AWS Batch.

Output Structure
----------------
CALDP and CALCLOUD output data in a form desgined to help track the state of individual datasets.

As such, the output directory is organized into two subdirectories:

1. *messages*
2. *data*

A key difference between CALDP and CALCLOUD is that the former is designed for processing single
datasets, while the latter is designed for processing batches of datasets which are run individually
by CALCLOUD. In this context, normally files downloaded from CALCLOUD's S3 storage to an onsite
directory are placed in a "batch directory", and the CALDP equivalent of that batch directory is
the output directory. The same messages and data appearing in the CALDP output directory would
also appeaar in the sync'ed CALCLOUD batch directory.

Messages Subdirectory
=====================
The *messages* subdirectory is used to record the status of individual datasets
as they progress through processing, data transfer, and archiving. Each dataset has a
similarly named state file which moves between state directories as it starts or completes
various states. The dataset file can be used to record metadata but its primary use
is to enable simple indentification dataset state without the use of a database, queues,
etc. Only a local file system is needed to track state using this scheme. A mirror
of this same scheme is used on the cloud on S3 storage to help guide file downloads from
AWS.

.. code-block:: sh

    <output_path>/
        messages/
            datasets-processed/
                <ipppssoots...>    # CALDP, normally running on AWS batch, leaves messages here. they're empty.
            dataset-synced/
                <ipppssoots...>    # CALCLOUD's downloader leaves messages here, normally containing abspaths of files to archive.
            dataset-archived/
                <ipppssoots...>    # The archive can acknowledge archive completion here, file contents should be preserved.

Data Subdirectory
=================
The *data* subdirectory parallels but has a different structure than the *messages*
subdirectory. For every ipppssoot message, there is a data directory and subdirectories
which contain output files from processsing that ipppssoot. In the current implementation,
the ipppssoot message file is empty, it is normally populated by CALCLOUD's downloader
with the paths of files to archive when it is output to dataset-synced.

.. code-block:: sh

    <output_path>/
        data/
            <instrument>/
                <ipppssoots...>/    # one dir per ipppssoot
                    science data files for one ipppssoot...
                    logs/
                        log and metrics files for one ipppssoot...
                    previews/
                        preview images for one ipppssoot...

Configuring CALDP (advanced)
----------------------------
As explained previously, each of the 3 CALDP use cases has a different CRDS configuration.
This implementation is described here in case it is necessary to write additional configurations
or add variables to these. At present, unlike *caldp-image-config*, these config scripts
don't generally need customization, they are used as-is to support their use cases.

CALDP configuration scripts set environment variables which will be defined within the scope
of *caldp-process*. These configuration scripts are installed alongside other CALDP scripts so they
can be sourced directly without knowing where they are installed. The name of the
configuration script is passed as a 4th generally defaulted parameter to caldp-process:

.. csv-table::
    :header: "Top Level Script",  "Config Script", "Description"
    :widths: 15, 15, 50

    caldp-process, caldp-config-onsite, Configures CRDS to operate from Central Store /grp/crds/cache. Should scale.
    caldp-docker-run-pipeline, caldp-config-offsite, Configures CRDS to download from CRDS server. This may not scale well.
    caldp-process-aws, caldp-config-aws, Configures CRDS to operate from S3 storage with no server dependency. Should scale.

Testing
-------

Travis
======

The CALDP repo is set up for Travis via github checkins.   Whenever you do a PR to spacetelescope/caldp,
Travis will automatically run CI tests for CALDP.

Native Testing
==============

It's common to do testing on a development machine prior to pushing.   This can basically be
accomplished by installing caldp,  configuring your environment, and then running pytest
similar to how it will be run by Travis.

.. code-block:: sh

    # FIRST: Setup a conda environment for CALDP as discussed above in native installs.
    # Don't use the "everything install" if you have an existing conda environment you
    # don't want to wipe out.   Make sure to activate it.

    # THEN:  configure your environment and run pytest as Travis would:
    source caldp-config-offsite
    pytest caldp --cov=caldp --cov-fail-under 80  --capture=tee-sys

**NOTE:** Not all CALDP code and capabilities are tested, particularly the wrapper scripts
currently associated with running the Python package inside and outside Docker.

S3 I/O
======

Because S3 inputs and outputs require AWS credentials to enable access, and specific object paths
to use,  testing of S3 modes is controlled by two environment variables which define where to locate
S3 inputs and outputs:

.. code-block:: sh

    export CALDP_S3_TEST_INPUTS=s3://caldp-hst-test/inputs/test-batch
    export CALDP_S3_TEST_OUTPUTS=s3://caldp-hst-test/outputs/test-batch

If either or both of the above variables is defined, pytest will also execute tests which utilize the S3
input or output modes.  You must also have AWS credentials for this.  Currently S3 is not tested on Travis.



