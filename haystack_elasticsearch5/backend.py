from haystack.backends.elasticsearch_backend import ElasticsearchSearchBackend, ElasticsearchSearchQuery
from haystack.backends import BaseEngine


__all__ = ['Elasticsearch5SearchBackend', 'Elasticsearch5SearchEngine']


class Elasticsearch5SearchBackend(ElasticsearchSearchBackend):
    pass


class Elasticsearch5SearchQuery(ElasticsearchSearchQuery):
    pass

class Elasticsearch5SearchEngine(BaseEngine):
    backend = ElasticsearchSearchBackend
    query = Elasticsearch5SearchQuery
