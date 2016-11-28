import elasticsearch

import datetime
from decimal import Decimal

from django.test import TestCase
from django.conf import settings

from haystack import connections, indexes# , reset_search_queries
from haystack.utils.loading import UnifiedIndex
from haystack.exceptions import SkipDocument

from tests.test_app.models import AFourthMockModel, AnotherMockModel, ASixthMockModel, MockModel

# TODO implement get_connection_params
# import sys
# import ipdb; ipdb.set_trace()


# TODO move to utils
def clear_elasticsearch_index():
    # Wipe it clean.
    raw_es = elasticsearch.Elasticsearch(settings.HAYSTACK_CONNECTIONS['default']['URL'])

    try:
        raw_es.indices.delete(index=settings.HAYSTACK_CONNECTIONS['default']['INDEX_NAME'])
        raw_es.indices.refresh()
    except elasticsearch.TransportError:
        pass

    # Since we've just completely deleted the index, we'll reset setup_complete so the next access will
    # correctly define the mappings:
    connections['default'].get_backend().setup_complete = False


class ElasticsearchMockSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True, use_template=True)
    name = indexes.CharField(model_attr='author', faceted=True)
    pub_date = indexes.DateTimeField(model_attr='pub_date')

    def get_model(self):
        return MockModel


class ElasticsearchMockSearchIndexWithSkipDocument(ElasticsearchMockSearchIndex):

    def prepare_text(self, obj):
        if obj.author == 'daniel3':
            raise SkipDocument
        return u"Indexed!\n%s" % obj.id


class ElasticsearchMockSpellingIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True)
    name = indexes.CharField(model_attr='author', faceted=True)
    pub_date = indexes.DateTimeField(model_attr='pub_date')

    def get_model(self):
        return MockModel

    def prepare_text(self, obj):
        return obj.foo


class ElasticsearchMaintainTypeMockSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True, use_template=True)
    month = indexes.CharField(indexed=False)
    pub_date = indexes.DateTimeField(model_attr='pub_date')

    def prepare_month(self, obj):
        return "%02d" % obj.pub_date.month

    def get_model(self):
        return MockModel


class ElasticsearchMockModelSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(model_attr='foo', document=True)
    name = indexes.CharField(model_attr='author')
    pub_date = indexes.DateTimeField(model_attr='pub_date')

    def get_model(self):
        return MockModel


class ElasticsearchAnotherMockModelSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True)
    name = indexes.CharField(model_attr='author')
    pub_date = indexes.DateTimeField(model_attr='pub_date')

    def get_model(self):
        return AnotherMockModel

    def prepare_text(self, obj):
        return u"You might be searching for the user %s" % obj.author


class ElasticsearchBoostMockSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(
        document=True, use_template=True,
        template_name='search/indexes/core/mockmodel_template.txt'
    )
    author = indexes.CharField(model_attr='author', weight=2.0)
    editor = indexes.CharField(model_attr='editor')
    pub_date = indexes.DateTimeField(model_attr='pub_date')

    def get_model(self):
        return AFourthMockModel

    def prepare(self, obj):
        data = super(ElasticsearchBoostMockSearchIndex, self).prepare(obj)

        if obj.pk == 4:
            data['boost'] = 5.0

        return data


class ElasticsearchFacetingMockSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True)
    author = indexes.CharField(model_attr='author', faceted=True)
    editor = indexes.CharField(model_attr='editor', faceted=True)
    pub_date = indexes.DateField(model_attr='pub_date', faceted=True)
    facet_field = indexes.FacetCharField(model_attr='author')

    def prepare_text(self, obj):
        return '%s %s' % (obj.author, obj.editor)

    def get_model(self):
        return AFourthMockModel


class ElasticsearchRoundTripSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True, default='')
    name = indexes.CharField()
    is_active = indexes.BooleanField()
    post_count = indexes.IntegerField()
    average_rating = indexes.FloatField()
    price = indexes.DecimalField()
    pub_date = indexes.DateField()
    created = indexes.DateTimeField()
    tags = indexes.MultiValueField()
    sites = indexes.MultiValueField()

    def get_model(self):
        return MockModel

    def prepare(self, obj):
        prepped = super(ElasticsearchRoundTripSearchIndex, self).prepare(obj)
        prepped.update({
            'text': 'This is some example text.',
            'name': 'Mister Pants',
            'is_active': True,
            'post_count': 25,
            'average_rating': 3.6,
            'price': Decimal('24.99'),
            'pub_date': datetime.date(2009, 11, 21),
            'created': datetime.datetime(2009, 11, 21, 21, 31, 00),
            'tags': ['staff', 'outdoor', 'activist', 'scientist'],
            'sites': [3, 5, 1],
        })
        return prepped


class ElasticsearchComplexFacetsMockSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True, default='')
    name = indexes.CharField(faceted=True)
    is_active = indexes.BooleanField(faceted=True)
    post_count = indexes.IntegerField()
    post_count_i = indexes.FacetIntegerField(facet_for='post_count')
    average_rating = indexes.FloatField(faceted=True)
    pub_date = indexes.DateField(faceted=True)
    created = indexes.DateTimeField(faceted=True)
    sites = indexes.MultiValueField(faceted=True)

    def get_model(self):
        return MockModel


class ElasticsearchAutocompleteMockModelSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(model_attr='foo', document=True)
    name = indexes.CharField(model_attr='author')
    pub_date = indexes.DateTimeField(model_attr='pub_date')
    text_auto = indexes.EdgeNgramField(model_attr='foo')
    name_auto = indexes.EdgeNgramField(model_attr='author')

    def get_model(self):
        return MockModel


class ElasticsearchSpatialSearchIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(model_attr='name', document=True)
    location = indexes.LocationField()

    def prepare_location(self, obj):
        return "%s,%s" % (obj.lat, obj.lon)

    def get_model(self):
        return ASixthMockModel


class TestSettings(TestCase):

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
    def test_non_silent(self):
        bad_sb = connections['default'].backend('bad', URL='http://omg.wtf.bbq:1000/', INDEX_NAME='whatver', SILENTLY_FAIL=False, TIMEOUT=1)

        try:
            bad_sb.update(self.smmi, self.sample_objs)
            self.fail()
        except:
            pass

        try:
            # import ipdb; ipdb.set_trace()
            bad_sb.remove('core.mockmodel.1')
            self.fail()
        except:
            pass

        try:
            bad_sb.clear()
            self.fail()
        except:
            pass

        try:
            bad_sb.search('foo')
            self.fail()
        except:
            pass

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

    def test_update(self):
        pass

    def test_update_with_SkipDocument_raised(self):
        pass
    def test_remove(self):
        pass
    def test_remove_succeeds_on_404(self):
        pass
    def test_clear(self):
        pass
    def test_search(self):
        pass
    def test_spatial_search_parameters(self):
        pass
    def test_more_like_this(self):
        pass
    def test_build_schema(self):
        pass
    def test_verify_type(self):
        pass
