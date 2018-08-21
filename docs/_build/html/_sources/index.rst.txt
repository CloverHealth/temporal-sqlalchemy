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

Setup
~~~~~

To set up your development environment, you'll need to install a few things.
For Python version management, we use `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`_.
Follow the installation instructions there, and then in the *root directory* of
this repo run: ``make setup``

Running the Tests
~~~~~~~~~~~~~~~~~

To run the unit tests for all supported versions of Python, run ``make test``. If you
made a change to the package requirements (in ``setup.py`` or ``test_requirements.txt``)
then you'll need to rebuild the environment.


Usage
-----

I'll start with the caveats, as there are a few:

* you cannot track the history of properties with ``onupdate``, ``server_default``
  or ``server_onupdate``

.. code:: python

    import sqlalchemy as sa
    import temporal_sqlalchemy as temporal

    class MyModel(temporal.TemporalModel, SomeBase):
        id = sa.Column(sa.BigInteger, primary_key=True)
        # this will throw an error
        prop_a = sa.Column(sa.Text, onupdate='Some Update Value')
        # this will also throw an error
        prop_b = sa.Column(sa.Text, server_default='Some Server Default')

* you have to "temporalize" your session

.. code:: python

    import sqlalchemy.orm as orm
    import temporal_sqlalchemy as temporal

    sessionmaker = orm.sessionmaker()
    session = sessionmaker()
    temporal.temporal_session(session)

    foo = MyModel(prop_a='first value', prop_b='also first first value')
    session.add(foo)
    session.commit()


    with foo.clock_tick():
        foo.prop_a = 'new value'
        foo.prop_b = 'also new value'

    session.commit()

* default values are tricky. If you need to record the state of a default
  value, or need `None` to have historical meaning you must be deliberate.
  Additionally, we cannot currently handle callable defaults that take a
  context
  `described here <http://docs.sqlalchemy.org/en/rel_1_0/core/defaults.html#context-sensitive-default-functions>`_

.. code:: python

    import sqlalchemy as sa
    import sqlalchemy.orm as orm
    import temporal_sqlalchemy as temporal

    sessionmaker = orm.sessionmaker()
    session = sessionmaker()
    temporal.temporal_session(session)


    class MyModel(temporal.TemporalModel, SomeBase):
        __tablename__ = 'my_model_table'
        __table_args__ = {'schema': 'my_schema'}

        id = sa.Column(sa.BigInteger, primary_key=True)
        description = sa.Column(sa.Text)

        class Temporal:
            track = ('description', )


    m = MyModel()
    session.add(m)
    session.commit()

    assert m.vclock == 1
    assert m.description == None

    description_hm = temporal.get_history_model(MyModel.description)

    history = session.query(description_hm).filter(description_hm.entity==m)

    # no history entry is created!
    assert history.count() == 0

    # do this instead
    m2 = MyModel(description=None)
    session.add(m2)
    session.commit()

    assert m2.vclock == 1
    assert m2.description == None

    history = session.query(description_hm).filter(description_hm.entity==m2)

    # history entry is now created
    assert history.count() == 1

Using Your Model
----------------

.. code:: python

    import sqlalchemy.orm as orm
    import temporal_sqlalchemy as temporal

    sessionmaker = orm.sessionmaker()
    session = sessionmaker()

    temporal.temporal_session(session)
    instance = MyModel(description="first description")

    assert instance.vclock == 1

    session.add(instance)
    session.commit()

Updating your instance
----------------------

.. code:: python

    with instance.clock_tick():
        instance.description = "second description"

    assert instance.vclock = 2
    session.commit()

Inspecting history
------------------

.. code:: python

    import temporal_sqlalchemy as temporal

    description_hm = temporal.get_history_model(MyModel.description)

    history = session.query(description_hm).filter(description_hm.entity==instance)

    assert history.count() == 2
    assert history[0].description == 'first description'
    assert history[1].description == 'second description'
