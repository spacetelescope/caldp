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

Gitflow
-------

This repository is organized under the `Gitflow <https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow>`_
model. Feature branches can be PR'ed into ``develop`` from forks. To the extent that
is reasonable, developers should follow these tenets of the Gitflow model:

- feature branches should be started off of ``develop``, and PR'ed back into ``develop``
- release candidates should branch off of ``develop``, be PR'ed into ``main``, and
  merged back into ``develop`` during final release.
- hotfixes should branch off of ``main``, be PR'ed back to ``main``, and be merged back
  to ``develop`` after release.

While developers are free to work on features in their forks, it is preferred for releases
and hotfixes to be prepared via branches on the primary repository.

Our github action workflow ``merge-main-to-develop`` runs after any push to ``main``,
(which automatically includes merged PR's). In practice this is a slight deviation
from Gitflow, which would merge the release or hotfix branch into ``develop``. However,
due to the nature of github action permissions, the github action triggered by a PR from
a fork does not have sufficient scope to perform that secondary merge directly from the
PR commit. This security limitation would require a personal access token of an admin to
be added to the account to allow github actions to merge. By merging from ``main`` right
after push, the github action has sufficient privilege to push to ``develop``. The
implication being that the security of code added via PR from a fork falls on the
administrators of this project, and is not inadvertently circumvented via github action
elevated privileges.

Overview of CALDP
-----------------

CALDP integrates fundamental HST calibration programs (*e.g.* calacs.e) with
input data, output data, and calibration reference files (CRDS).

CALDP does end-to-end calibration of HST data in a manner similar to the
archive pipeline, including the generation of preview images.   It is currently
used for cloud-based reprocessing of HST data.

CALDP includes a Python package `caldp` which runs more fundamental calibration
programs to perform numerical processing.  The `caldp` program/package is in
turn wrapped by the top level script `caldp-process`.

CALDP is used to build upon an upstream Docker image by adding its Python
package, small amounts of additional software, and configuration scripts.  The
main program run by Docker is `caldp-process`.

 Development Quickstart
-----------------------

This section walks quickly through the installation and/or development and test
process.

Prerequisites (Docker)
======================

Our recommended approach for doing CALDP development is to run or test solely
within the final CALDP container environment.  This simplifies building and
configuration and ensures you are testing a final produt.

Because of the reliance on Docker, a local copy of miniconda is no longer a
requirement for doing development.

Installing
==========

Currently CALDP is only installed from source code,  there is no higher
level packaging such as PyPi:

.. code-block:: sh

    git clone https://github.com/spacetelescope/caldp.git
    cd caldp

Configuring
===========

Top level configuration settings are defined in the file `caldp-setup` in the
root directory of the repo.  These work out-of-the-box for e.g. laptop
development or GitHub and can be tweaked for other use cases:

.. code-block:: sh

    source caldp-setup

Building the Image
==================

Any processing or testing first requires building the complete CALDP Docker
image:

.. code-block:: sh

    caldp-image-build

Pushing the Image (on AWS)
==========================

If applicable, push your image to ECR for retrieval by AWS Batch:

.. code-block:: sh

    caldp-ecr-login  <admin-role>
    caldp-image-push

Pushing is not required for local development testing.

Running Pytests and Coverage
============================

Tests are run inside the Docker container, nominally testing the exact
image that will be run in HST repro (provided the image is delivered
to repro):

.. code-block:: sh

    caldp-test

Debugging
=========

It's possible and easy to work inside a running Docker container
interactively.  Whatever directory you expose via CALDP_HOME will
map into the container r/w as /home/developer.  By default CALDP_HOME
is set to your host computer's clone of the CALDP repo.

.. code-block:: sh

    caldp-sh    # start an interactive shell in the container

    caldp-sh printenv   # print the shell environment in Docker

    caldp-sh <any program and parameters to run in docker...>


Native Runs
-----------

This section describes the top level entrypoint for the CALDP
program,  i.e. what is ultimately developed and tested,  whether
inside or outside Docker:

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
=======================

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

    # ----------------------------------------------------------------------------------------
    # Download inputs from s3, upload outputs to S3 (AWS credentials and permission required)
    # Inputs: Downloads compressed (tar.gz) file matching j8cb010b0 from s3 and extracts to folder in the current working directory / CALDP_HOME/j8cb010b0/.
    # Outputs: Copies output product tree to AWS S3 storage bucket.
    # CRDS configuration: VPN configuration, no CRDS server required, /grp/crds/cache must be visible.
    # Scratch files: Extra processing artifacts appear in CALDP_HOME/j8cb010b0/. Export CALDP_HOME to move them somewhere else.

    caldp-process j8cb010b0  s3://calcloud-hst-pipeline-inputs  s3://calcloud-hst-pipeline-outputs


Getting AWS Credentials Inside the Container
============================================
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

Error Handling
--------------

Exit Codes
==========

CALDP runs a sequence of steps and programs to fully process each dataset.
Every program has its own methods of error handling and reporting failures.
One limitation of AWS Batch is that **the only CALDP status communicated
directly back to Batch is the numerical program exit code.** There is a
universal convention that a program which exits with a non-zero return status
has failed; conversely a status of zero indicates success.  There is no
convention about what non-zero exit code values should be, they vary program by
program.  It should be noted that Python and Batch have different methods of
displaying the same one byte exit code, unsigned byte for Python, integer for
Batch.

CALDP error code meanings can only be found in the program logs or in
*caldp/exit_codes.py*.  In contrast, AWS Batch reports text descriptions in
addition to numerical exit codes, but only for failures at the Batch level,
such as Docker failures.

CALCLOUD Error Handling
=======================

A CALCLOUD Batch event handler is triggered upon CALDP job failure.  The event
handler interprets the combination of CALDP exit code, Batch exit code, and
Batch exit reason to determine the error type and react appropriately.
Reactions include automatically rescuing jobs with memory errors, retrying
Docker failures, recording error-ipppssoot messages, etc.

Normalizing Error Codes
=======================

Because there is uncertainty about how each subprogram chooses to define exit
codes,  and to give the batch event handler more information for decision
making,  CALDP often brackets blocks of code like this:

.. code-block:: python

  with sysexit.exit_on_exception(caldp_exit_code, "descriptive message"):
      ... python statements ...

such that an exception raised by the nested statements is caught and thrown to
the *exit_receiver()* handler,  typically at the highest program level:

.. code-block:: python

  with sysexit.exit_reciever():
      main()

The *exit_receiver()* intercepts the chain of unwinding handlers, squelches the
traceback between *exit_on_exception()* and *exit_receiver()*, then calls
*sys._exit(caldp_exit_code)* to exit immediately. In this manner, caldp reports
the error code *caldp_exit_code* rather than any code assigned by a subprogram.

Currently three different failure modes involving memory errors are mapped onto
the same CALCLOUD job rescue handling: Python MemoryError, Unreported but
logged subprogram Python MemoryError, Container memory error.  This illustrates
how characterization and handling are sometimes just... ugly.

Codes are assigned to specific functional blocks in the hope that as new
failure modes are observed, handling can be added to CALCLOUD without changing
CALDP.  However, when necessary, exception bracketing should be revised, new
error codes should be added, and the modified *exit_codes.py* module should be
copied to CALCLOUD which may also need handling updates.

**NOTE:**  AWS Batch also issues numerical exit codes so while there are no known
cases of overlap,  there is a potential for amiguity between Batch and CALDP,
but not for CALDP subprograms.

Testing
-------

GitHub Actions
==============

The CALDP repo is set up for GitHub Actions with the following workflows:

- docker: Docker build and test,  both pytest and simple caldp-process
- check:  flake8, black, and bandit checks

Whenever you do a PR or merge to spacetelescope/caldp, GitHub will
automatically run CI tests for CALDP.

Additionally, there are several workflows that aid in managing the
`Gitflow <https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow>`_
workflow.

- tag-latest: automatically tags the latest commit to ``develop`` as ``latest``
- tag-stable: automatically tags the latest commit to ``main`` as ``stable``
- merge-main-to-develop: merges ``main`` back down to ``develop`` after any push to ``main``
- check-merge-main2develop: checks for merge failures with ``develop``, for any PR to ``main``.
  For information only; indicates that manual merge conflict resolution may be required
  to merge this PR back into ``develop``. Not intended to block PR resolution, and no attempt
  to resolve the conflict is needed prior to merging ``main``.


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
