from django.test import TestCase
# from django.conf import settings


class TestSettings(TestCase):

    def setUp(self):
        from haystack_elasticsearch5 import Elasticsearch5SearchBackend

        self.backend = Elasticsearch5SearchBackend('alias', **{
            'URL': 'http://11.22.33.44:3333/',
            'INDEX_NAME': 'whocare',
            'KWARGS': {'max_retries': 42}
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


# class Elasticsearch5BackendTest(TestCase):

#     def test_foo(self):

#         raise Exception('You got me!')
