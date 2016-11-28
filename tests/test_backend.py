import elasticsearch

import datetime

from django.test import TestCase
from django.conf import settings

from haystack import connections# , reset_search_queries
from haystack.utils.loading import UnifiedIndex

from tests.test_app.models import MockModel
from tests.test_app.search_indexes import (ElasticsearchMockSearchIndex,
                                           ElasticsearchMockSearchIndexWithSkipDocument,
                                           ElasticsearchMaintainTypeMockSearchIndex)

from .utils import clear_elasticsearch_index


# TODO implement get_connection_params
# import sys
# import ipdb; ipdb.set_trace()


class TestTheSettings(TestCase):

    def setUp(self):
        from haystack_elasticsearch5 import Elasticsearch5SearchBackend

        self.backend = Elasticsearch5SearchBackend('alias', **{
            'URL': 'http://11.22.33.44:3333/', # TODO check https
            'INDEX_NAME': 'whocare',
            'KWARGS': {'max_retries': 42} # TODO check another settings?
        })

    def test_kwargs_are_passed_on(self):

        self.assertEqual(self.backend.conn.transport.max_retries, 42)

    def test_url_are_passed_on(self):

        hosts = self.backend.conn.transport.hosts[0] # we passed parameters for only one connection
        expected = {'scheme': 'http', 'host': '11.22.33.44', 'port': 3333}

        self.assertEqual(hosts, expected)

    def test_alias_are_passed_on(self):

        self.assertEqual(self.backend.connection_alias, 'alias')

    def test_index_name_are_passed_on(self):

        self.assertEqual(self.backend.index_name, 'whocare')


class Elasticsearch5BackendTest(TestCase):

    def setUp(self):

        super(Elasticsearch5BackendTest, self).setUp()

        # Wipe it clean.
        self.raw_es = elasticsearch.Elasticsearch(settings.HAYSTACK_CONNECTIONS['default']['URL'])
        clear_elasticsearch_index()

        # Stow.
        self.old_ui = connections['default'].get_unified_index()
        self.ui = UnifiedIndex()
        self.smmi = ElasticsearchMockSearchIndex()
        self.smmidni = ElasticsearchMockSearchIndexWithSkipDocument()
        self.smtmmi = ElasticsearchMaintainTypeMockSearchIndex()

        self.ui.build(indexes=[self.smmi])

        connections['default']._index = self.ui
        self.sb = connections['default'].get_backend()

        # Force the backend to rebuild the mapping each time.
        self.sb.existing_mapping = {}
        self.sb.setup()

        self.sample_objs = []

        for i in range(1, 4):
            mock = MockModel()
            mock.id = i
            mock.author = 'daniel%s' % i
            mock.pub_date = datetime.date(2009, 2, 25) - datetime.timedelta(days=i)
            self.sample_objs.append(mock)

    # TODO improve; wtf is this do?
    # def test_non_silent(self):
    #     bad_sb = connections['default'].backend('bad', URL='http://omg.wtf.bbq:1000/', INDEX_NAME='whatver', SILENTLY_FAIL=False, TIMEOUT=1)

    #     try:
    #         bad_sb.update(self.smmi, self.sample_objs)
    #         self.fail()
    #     except:
    #         pass

    #     try:
    #         # import ipdb; ipdb.set_trace()
    #         bad_sb.remove('core.mockmodel.1')
    #         self.fail()
    #     except:
    #         pass

    #     try:
    #         bad_sb.clear()
    #         self.fail()
    #     except:
    #         pass

    #     try:
    #         bad_sb.search('foo')
    #         self.fail()
    #     except:
    #         pass

    def test_update_if_there_is_no_documents_and_silently_fail(self):
        # TODO implement get_connection_params
        url = settings.HAYSTACK_CONNECTIONS['default']['URL']
        index_name = settings.HAYSTACK_CONNECTIONS['default']['INDEX_NAME']
        documents = []

        # import ipdb; ipdb.set_trace()
        search_backend = connections['default'].backend('default', URL=url, INDEX_NAME=index_name, SILENTLY_FAIL=True)

            # update(index, iterable, commit=True)
        self.assertEqual(search_backend.update(self.smmi, documents), None)

    def test_update_if_there_is_no_documents_and_not_silently_fail(self):
        # TODO implement get_connection_params
        url = settings.HAYSTACK_CONNECTIONS['default']['URL']
        index_name = settings.HAYSTACK_CONNECTIONS['default']['INDEX_NAME']
        documents = []

        search_backend = connections['default'].backend('default', URL=url, INDEX_NAME=index_name, SILENTLY_FAIL=False)
        search_backend.update(self.smmi, documents)

    # def test_update(self):
    #     pass

    # def test_update_with_SkipDocument_raised(self):
    #     pass
    # def test_remove(self):
    #     pass
    # def test_remove_succeeds_on_404(self):
    #     pass
    # def test_clear(self):
    #     pass
    # def test_search(self):
    #     pass
    # def test_spatial_search_parameters(self):
    #     pass
    # def test_more_like_this(self):
    #     pass
    # def test_build_schema(self):
    #     pass
    # def test_verify_type(self):
    #     pass
