
import datetime

from haystack.exceptions import SkipDocument
from haystack import indexes

from decimal import Decimal
from tests.test_app.models import AFourthMockModel, AnotherMockModel, ASixthMockModel, MockModel


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
    text = indexes.CharField( document=True, use_template=True,
        template_name='search/indexes/test_app/mockmodel_template.txt'
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
    pub_date = indexes.DateField(model_attr='pub_date')
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
