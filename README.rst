Temporal SQLAlchemy
===================

Some Terms
----------

Temporal
  time dimension. A property can have spatial and/or temporal dimensions,
  e.g. what it was and when it was that.

Bi-temporal
  2x time dimensions. Allows you to separate the "valid time" of a
  property from "when we knew about it"

Version Clock
  a simple, incrementing integer that allows you track "generation" of
  changes (also known as "vclock")

Version
  the state of an entity at a given version clock

Low Velocity
  state changes infrequently or you don't need to walk the history regularly

High Velocity
  state changes frequently or you need to walk the history regularly.

Methodologies
-------------

There exist several ways to add a temporal dimension to your data.
`SQLAlchemy Continuum`_ uses a shadow or history table for each versioned
entity.  You can add a :code:`date_created` and :code:`date_modified`
columns to your model.

.. _SQLAlchemy Continuum: https://SQLAlchemy-continuum.readthedocs.org/en/latest/

**Temporal SQLAlchemy** uses a table per property, with an entity clock -- all
state is written to the entity table as expected, but additionally recorded
into series of history tables (per property) and clock entries (per entity).

Testing and Development
-----------------------

This package uses `Tox <https://tox.readthedocs.io/en/latest/>`_ to run tests on
multiple versions of Python.

Setup
~~~~~

To set up your development environment, you'll need to install a few things.
For Python version management, we use `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`_.
Follow the installation instructions there, and then in the *root directory* of
this repo run:

.. code-block:: sh

    # Install all the Python versions this package supports. This will take some
    # time.
    pyenv install 3.3.6
    pyenv install 3.4.6
    pyenv install 3.5.3
    pyenv install 3.6.3

    pyenv local 3.6.3 3.5.3 3.4.6 3.3.6

    # Install the development dependencies
    pip3 install -Ur dev-requirements.txt

Running the Tests
~~~~~~~~~~~~~~~~~

To run the unit tests for all supported versions of Python, run ``tox``. If you
made a change to the package requirements (in ``setup.py`` or ``test_requirements.txt``)
then you'll need to rebuild the environment. Use ``tox -r`` to rebuild them and
run the tests.

Updating Version Numbers
~~~~~~~~~~~~~~~~~~~~~~~~

Once development is done, as your *last commit* on the branch you'll want to
change the version number and create a tag for deployment. Please do this via
the ``bumpversion`` command. More information on ``bumpversion`` and its usage
can be found `here <https://pypi.python.org/pypi/bumpversion>`_, but in most
cases you'll run one of the following commands. Assuming the current version is
1.2.3:

.. code-block:: sh

    # Change the version to 1.2.4
    bumpversion patch

    # Change the version to 1.3.0
    bumpversion minor

    # Change the version to 2.0.0
    bumpversion major
