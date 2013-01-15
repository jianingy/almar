#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : test_object_push_api.py
# created at : 2013-01-15 13:57:41
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

ALMAR_SERVER = "http://localhost:9999/"

import unittest
from jsonrpc import ServiceProxy, JSONRPCException
from copy import deepcopy

SERVICE_BASE = 'http://localhost:9999/object'


class ObjectPushTestCase(unittest.TestCase):

    service = None

    @classmethod
    def setUpClass(cls):

        service = ServiceProxy('http://localhost:9999/op')

        items = [dict(path='net.dot1q.dev30',
                      model='asset.netable',
                      value=dict(fqdn='dev30.dot1q.net',
                                 ip4='192.168.123.30',
                                 serialno='DRS030'))]
        cls.items = items
        service.upsert(items)

    @staticmethod
    def _get_object_service(path):
        from os.path import join
        service_uri = join(SERVICE_BASE, path.strip('.').replace('.', '/'))
        return ServiceProxy(service_uri)

    def test_push(self):
        service = self._get_object_service('net.dot1q.dev30')
        item = dict(model='asset.netable',
                    value=dict(fqdn='dev11.dot1q.net',
                               ip4='192.168.123.31',
                               serialno='DRS031'))

        resp = service.push([item])
        self.assertIn('affected', resp)
        self.assertEqual(resp['affected'], 1)

        service = self._get_object_service('net.dot1q.dev30.0')
        expect = deepcopy(item['value'])
        expect['__model__'] = item['model']
        self.assertEqual(service.get(), expect)

    def test_push_non_dict(self):
        service = self._get_object_service('net.dot1q.dev30')
        with self.assertRaises(JSONRPCException) as cm:
            service.push([list(), list()])

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('dict') > -1)

    def test_push_no_model(self):
        service = self._get_object_service('net.dot1q.dev30')
        item = dict(value=dict(fqdn='dev31.dot1q.net',
                               ip4='192.168.123.31',
                               serialno='DRS031'))
        with self.assertRaises(JSONRPCException) as cm:
            service.push([item])

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('missing') > -1)

    def test_push_invalid_model(self):
        service = self._get_object_service('net.dot1q.dev30')
        item = dict(model='something.not.exists',
                    value=dict(fqdn='dev32.dot1q.net',
                               ip4='192.168.123.32',
                               serialno='DRS032'))

        with self.assertRaises(JSONRPCException) as cm:
            service.push([item])

        e = cm.exception
        self.assertEqual(e.error['faultCode'], 4001)
        self.assertTrue(e.error['faultString'].find('does not exist') > -1)


if __name__ == '__main__':
    unittest.main()
