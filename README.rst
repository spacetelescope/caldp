The DMS environment for HST calibration software
------------------------------------------------

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge

This includes a framework for installing and testing the software that is used
to calibrate data from the Hubble Space Telescope


License
-------

This project is Copyright (c) Space Telescope Science Institute and licensed under
the terms of the Aura license. This package is based upon
the `Astropy package template <https://github.com/astropy/package-template>`_
which is licensed under the BSD 3-clause licence. See the licenses folder for
more information.


Contributing
------------

We love contributions! hstdp is open source,
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

*This disclaimer was originally written by
`Adrienne Lowe <https://github.com/adriennefriend>`_ for a
`PyCon talk <https://www.youtube.com/watch?v=6Uj746j9Heo>`_, and was adapted by
hstdp based on its use in the README file for the
`MetPy project <https://github.com/Unidata/MetPy>`_.*


JupyterHub Access
-----------------

*Note:* This is currently still in research-and-development stage and is subject to change.

To run a pre-installed pipeline in JupyterHub:

* Click on https://dev.science.stsci.edu/hub/spawn?image=793754315137.dkr.ecr.us-east-1.amazonaws.com/datb-tc-pipeline-nb:hstdp-snapshot and sign in.
* Click "Terminal" to:
    * Do a `which calacs.e` to see if CALACS is installed.
      You can repeat this for other HSTCAL executables, as desired.
    * Do a `calacs.e --version` to see which CALACS version is installed.
      You can repeat this for other HSTCAL executables, if applicable, as desired.
    * Run `pip freeze` to see what Python packages are installed (e.g., `calcos`).
    * Install any optional Python packages using `pip install`.
    * You can download the necessary data files using HTTP/HTTPS protocol.
    * Set up your `jref`, `iref`, etc. as desired.
    * Run the pipeline from command line.
    * Optional if you use Jupyter notebooks: Grab your notebooks (e.g., using `git clone`).
* Launch your notebook to run the pipeline.

Latest release of any packages is not guaranteed in this environment. Amazon Web Services charges may apply.
