
from django.apps import apps

from haystack.models import SearchResult


class MockSearchResult(SearchResult):
    def __init__(self, app_label, model_name, pk, score, **kwargs):
        super(MockSearchResult, self).__init__(app_label, model_name, pk, score, **kwargs)
        self._model = apps.get_model('test_app', model_name)

MOCK_SEARCH_RESULTS = [MockSearchResult('test_app', 'MockModel', i, 1 - (i / 100.0)) for i in range(1, 100)]
MOCK_INDEX_DATA = {}
