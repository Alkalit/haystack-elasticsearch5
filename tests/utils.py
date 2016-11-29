import elasticsearch

from haystack import connections
from django.conf import settings


def clear_elasticsearch_index():
    # Wipe it clean.
    raw_es = elasticsearch.Elasticsearch(settings.HAYSTACK_CONNECTIONS['default']['URL'])

    main_index_exists = raw_es.indices.exists(index=settings.HAYSTACK_CONNECTIONS['default']['INDEX_NAME'])

    if main_index_exists:
        raw_es.indices.delete(index=settings.HAYSTACK_CONNECTIONS['default']['INDEX_NAME'])
        raw_es.indices.refresh()

    # Since we've just completely deleted the index, we'll reset setup_complete so the next access will
    # correctly define the mappings:
    connections['default'].get_backend().setup_complete = False
