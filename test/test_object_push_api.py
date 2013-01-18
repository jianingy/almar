#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : test_object_push_api.py
# created at : 2013-01-11 14:25:07
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

ALMAR_SERVER = "http://localhost:9999/"

import unittest
from jsonrpc import ServiceProxy, JSONRPCException
from copy import deepcopy

SERVICE_BASE = 'http://localhost:9999/object'


class ObjectUpdateTestCase(unittest.TestCase):

    service = None

    @classmethod
    def setUpClass(cls):

        service = ServiceProxy('http://localhost:9999/op')

        items = [dict(path='net.dot1q.dev10',
                      model='asset.netable',
                      value=dict(fqdn='dev10.dot1q.net',
                                 ip4='192.168.123.10',
                                 serialno='DRS010'))]
        cls.items = items
        service.upsert(items)

    @staticmethod
    def _get_object_service(path):
        from os.path import join
        service_uri = join(SERVICE_BASE, path.strip('.').replace('.', '/'))
        return ServiceProxy(service_uri)

    def test_update(self):
        service = self._get_object_service('net.dot1q.dev11')
        item = dict(model='asset.netable',
                    value=dict(fqdn='dev11.dot1q.net',
                               ip4='192.168.123.11',
                               serialno='DRS011'))

        resp = service.update(item)
        self.assertIn('affected', resp)
        self.assertEqual(resp['affected'], 1)

        expect = deepcopy(item['value'])
        expect['__model__'] = item['model']
        self.assertEqual(service.get(), expect)

    def test_update_non_dict(self):
        service = self._get_object_service('net.dot1q.dev11')
        with self.assertRaises(JSONRPCException) as cm:
            service.update(list())

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('dict') > -1)

    def test_update_no_model_update(self):
        service = self._get_object_service('net.dot1q.dev11')
        item = dict(value=dict(fqdn='dev11b.dot1q.net',
                               ip4='192.168.123.11',
                               serialno='DRS011'))

        try:
            resp = service.update(item)
            self.assertIn('affected', resp)
            self.assertEqual(resp['affected'], 1)

            expect = deepcopy(item['value'])
            expect['__model__'] = 'asset.netable'
            self.assertEqual(service.get(), expect)
        except JSONRPCException as e:
            print e.error['faultString']

    def test_update_no_model_create(self):
        service = self._get_object_service('net.dot1q.dev13')
        item = dict(value=dict(fqdn='dev13.dot1q.net',
                               ip4='192.168.123.13',
                               serialno='DRS013'))
        with self.assertRaises(JSONRPCException) as cm:
            service.update(item)

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('missing') > -1)

    def test_update_invalid_model(self):
        service = self._get_object_service('net.dot1q.dev12')
        item = dict(model='something.not.exists',
                    value=dict(fqdn='dev12.dot1q.net',
                               ip4='192.168.123.12',
                               serialno='DRS012'))

        with self.assertRaises(JSONRPCException) as cm:
            service.update(item)

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('does not exist') > -1)

    def test_update_no_value(self):
        service = self._get_object_service('net.dot1q.dev11')
        item = dict(model='asset.netable')
        with self.assertRaises(JSONRPCException) as cm:
            service.update(item)

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('missing') > -1)

    def test_update_duplicated(self):
        service = self._get_object_service('net.dot1q.dev10b')
        item = dict(model='asset.netable',
                    value=dict(fqdn='dev10.dot1q.net',
                               ip4='192.168.123.10',
                               serialno='DRS010'))

        with self.assertRaises(JSONRPCException) as cm:
            service.update(item)

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 5001)
        self.assertTrue(e.error['faultString'].find('duplicate key') > -1)

    def test_update_invalid_value(self):
        service = self._get_object_service('net.dot1q.dev12')
        item = dict(model='asset.netable',
                    value=dict(fqdn='dev12.dot1q.net',
                               ip4='192.168.123.12',
                               a_very_funny_key='should not be inserted',
                               serialno='DRS012'))

        with self.assertRaises(JSONRPCException) as cm:
            service.update(item)

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('undefined key') > -1)


if __name__ == '__main__':
    unittest.main()
