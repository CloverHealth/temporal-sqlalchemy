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

There are many other ways to add a time dimension to your data, but for now
lets just focus on those three. Shadow history tables and
:code:`date_created`/:code:`date_modified` are excellent choices if the
changes to your data are low velocity. If you have no requirements around
surfacing the history for specific features, or the expected changes happen
infrequently, both should suffice. However, if you know your data changes a
lot or you need to walk the history to build features (who was this assigned
to before? when did it go from status a to status b?) you might want to consider using **Temporal SQLAlchemy**.
