=====================================
Elasticsearch 5 backend for haystack.
=====================================

Currently tested for python 3.5 and Django 1.10

Run tests
=========

Use ``tox`` or  ``django-admin test --settings=tests.settings``. In second case environment should have django and haystack installed.


How to port from elasticsearch 1
================================
- First, you need to rebuild index
- Remove ``faceted=True`` from fields in all your indexes.
