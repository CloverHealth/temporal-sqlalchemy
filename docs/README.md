# Temporal SQLAlchemy

## Usage

I'll start with the caveats, as there are a few:

* you cannot track the history of properties with `onupdate`, `server_default`
  or `server_onupdate`

```python
import sqlalchemy as sa
import temporal_sqlalchemy as temporal


class MyModel(temporal.TemporalModel, SomeBase):
    id = sa.Column(sa.BigInteger, primary_key=True)
    # this will throw an error
    prop_a = sa.Column(sa.Text, onupdate='Some Update Value')
    # this will also throw an error
    prop_b = sa.Column(sa.Text, server_default='Some Server Default')
```

* you have to "temporalize" your session

```python
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
```

* default values are tricky. If you need to record the state of a default
  value, or need `None` to have historical meaning you must be deliberate.
  Additionally, we cannot currently handle callable defaults that take a
  context
  [described here](http://docs.sqlalchemy.org/en/rel_1_0/core/defaults.html#context-sensitive-default-functions)

```python
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
```

### Using Your Model


```python
import sqlalchemy.orm as orm
import temporal_sqlalchemy as temporal

sessionmaker = orm.sessionmaker()
session = sessionmaker()

temporal.temporal_session(session)
instance = MyModel(description="first description")

assert instance.vclock == 1

session.add(instance)
session.commit()
```

### Updating your instance

```python

with instance.clock_tick():
    instance.description = "second description"

assert instance.vclock = 2
session.commit()

```

### inspecting history

```python
import temporal_sqlalchemy as temporal

description_hm = temporal.get_history_model(MyModel.description)

history = session.query(description_hm).filter(description_hm.entity==instance)

assert history.count() == 2
assert history[0].description == 'first description'
assert history[1].description == 'second description'
```
