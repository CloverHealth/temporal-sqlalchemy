import temporal_sqlalchemy as temporal

from . import shared, models


class TestEdgeCases(shared.DatabaseTest):

    def test_indentifiers_too_long(self):
        models.edgecase_metadata.create_all(self.connection)

        clock_table = models.HugeIndices.temporal_options.clock_table.__table__
        assert clock_table.name == (
            'testing_a_' + models.REALLY_REALLY + 'long_table_2907')

        history_model = temporal.get_history_model(
            models.HugeIndices.really_really_really_really_really_long_column)
        history_table = history_model.__table__

        assert history_table.name == (
            'testing_a_' + models.REALLY_REALLY + 'long_table_9e40')
        assert ('testing_a_' + models.REALLY_REALLY + 'long_table_3b65'
                in {idx.name for idx in history_table.indexes})
