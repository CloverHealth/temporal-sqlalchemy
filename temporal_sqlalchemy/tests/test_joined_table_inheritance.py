# pylint: disable=missing-docstring, no-self-use
import pytest
import sqlalchemy as sa

import temporal_sqlalchemy as temporal

from . import models


def test_joined_enums_create(session):
    session.add_all([
        models.JoinedEnumA(val='foo'),
        models.JoinedEnumA(val='foobar'),
        models.JoinedEnumB(val='bar'),
        models.JoinedEnumB(val='barfoo'),
        models.JoinedEnumB(val='barfoo'),
    ])

    session.commit()

    assert session.query(models.JoinedEnumBase).count() == 5

    enum_a_val_history = temporal.get_history_model(models.JoinedEnumA.val)
    assert session.query(enum_a_val_history).count() == 2

    enum_b_val_history = temporal.get_history_model(models.JoinedEnumB.val)
    assert session.query(enum_b_val_history).count() == 3


@pytest.mark.parametrize("model,first_val,second_val", (
        (models.JoinedEnumA, 'foo', 'foobar'),
        (models.JoinedEnumA, 'foobar', 'foo'),
        (models.JoinedEnumB, 'bar', 'barfoo'),
))
def test_joined_enums_edit(session, model, first_val, second_val):
    kind = sa.inspect(model).polymorphic_identity

    session.add(model(val=first_val, is_deleted=False))
    session.commit()

    entity = session.query(model).first()
    entity_id = entity.id
    with entity.clock_tick():
        entity.is_deleted = True
        entity.val = second_val

    session.commit()
    session.expunge_all()  # empty the session

    # query directly from the model
    entity = session.query(model).get(entity_id)
    assert entity.vclock == 2
    assert entity.val == second_val
    assert entity.kind == kind
    assert entity.is_deleted is True

    # query via with_polymorphic
    entity = session.query(models.JoinedEnumBase)\
        .with_polymorphic(model).first()
    assert entity.vclock == 2
    assert entity.val == second_val
    assert entity.kind == kind
    assert entity.is_deleted is True
