=====================================
Elasticsearch 5 backend for haystack.
=====================================

Currently tested for python 3.5 and Django 1.10

How to install
==============

``pip install django-haystack-elasticsearch5``

In your ``settings.py``


::

    HAYSTACK_CONNECTIONS = {
        'default': {
            'ENGINE': 'haystack_elasticsearch5.Elasticsearch5SearchEngine',
            'URL': 'http://127.0.0.1:9200/',
            'INDEX_NAME': 'test_backend',
        }
    }


Run tests
=========

Use ``tox`` or  ``django-admin test --settings=tests.settings``. In second case environment should have django and haystack installed.


How to port from elasticsearch 1
================================
- Remove ``faceted=True`` from fields in all your indexes. And you probably will need to replace all ``FacetNameField`` common analogue.
- Then rebuild index
