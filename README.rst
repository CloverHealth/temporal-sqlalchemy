Temporal SQLAlchemy
-------------------

Why
===

As a Medicare Advantage company, Clover is beholden to rather stringent auditability requirements.
Additionally, there is a need as a *data driven organization* to have a solid understanding of
our data and how it changes over time.

Some Terms
==========

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
=============

There exist several ways to add a temporal dimension to your data.
`SQLAlchemy Continuum`_ uses a shadow or history table for each versioned
entity.  You can add a :code:`date_created` and :code:`date_modified`
columns to your model.

.. _SQLAlchemy Continuum: https://SQLAlchemy-continuum.readthedocs.org/en/latest/

**Temporal SQLAlchemy** uses a table per property, with an entity clock -- all
state is written to the entity table as expected, but additionally recorded
into series of history tables (per property) and clock entries (per entity).
