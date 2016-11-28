from django.conf import settings
from elasticsearch import Elasticsearch, ElasticsearchException


es = Elasticsearch(settings.HAYSTACK_CONNECTIONS['default']['URL'])

try:
    es.info()
except ElasticsearchException:
    raise ElasticsearchException("There is no elasticsearch node running on {}".format(settings.HAYSTACK_CONNECTIONS['default']['URL']))

version = es.info()['version']['number']
major_version = version.split('.')[0]

if not int(major_version) == 5:
    raise ElasticsearchException("ES version is not 5, but {} instead.".format(version))
