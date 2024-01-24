Hello World with MPI+Parsl on Rivanna
=====================================

This repository contains simple "hello world" example
to run mpi code on Rivanna using parsl.

We use Conda for managing software dependencies.

We expect that the user of the pipeline will install Miniconda
in their home directory on every compute cluster.
Installation instructions for Miniconda can be found
`here <https://docs.conda.io/en/latest/miniconda.html>`_.

Once setup is done, please ensure that your conda config contains the following:

.. code::

  # ~/.condarc

  channels:
    - conda-forge
    - defaults
  anaconda_upload: false
  auto_activate_base: false

Please ensure you have the latest version of conda.

Create the new conda environment and add dependencies.

.. code:: bash

  # Create a conda environment with just python
  $ conda create -n parsl-test python=3.11
  $ conda activate parsl-test

  # Clone the git repository
  $ git clone https://github.com/NSSAC/parsl-mpi-hello-world-rivanna
  $ cd parsl-mpi-hello-world-rivanna

  # Make the MPI executable
  $ module load gcc/11.4.0 openmpi/4.1.4
  $ make

  # Install the workflow depndencies
  $ pip install -r requirements.txt

  # Edit config_rivanna.py to fix the account details

  # Run parsl_main.py
  $ ./parsl_main.py run


