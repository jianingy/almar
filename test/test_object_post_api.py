#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : test_rest.py
# created at : 2013-01-11 14:25:07
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

ALMAR_SERVER = "http://localhost:9999/"

import unittest
from jsonrpc import ServiceProxy, JSONRPCException
from copy import deepcopy

SERVICE_BASE = 'http://localhost:9999/object'


class ObjectPostTestCase(unittest.TestCase):

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

    def test_post(self):
        service = self._get_object_service('net.dot1q.dev11')
        item = dict(model='asset.netable',
                    value=dict(fqdn='dev11.dot1q.net',
                               ip4='192.168.123.11',
                               serialno='DRS011'))

        resp = service.update(item)
        self.assertIn('affected', resp)
        self.assertEqual(resp['affected'], 1)

    def test_post_non_dict(self):
        service = self._get_object_service('net.dot1q.dev11')
        try:
            service.update(list())
        except JSONRPCException as e:
            self.assertEqual(e.error['faultCode'], 4001)
            self.assertTrue(e.error['faultString'].find('dict') > -1)

    def test_post_no_model(self):
        service = self._get_object_service('net.dot1q.dev11')
        item = dict(value=dict(fqdn='dev11.dot1q.net',
                               ip4='192.168.123.11',
                               serialno='DRS011'))
        try:
            service.update(item)
        except JSONRPCException as e:
            self.assertEqual(e.error['faultCode'], 4001)
            self.assertTrue(e.error['faultString'].find('missing') > -1)

    def test_post_no_value(self):
        service = self._get_object_service('net.dot1q.dev11')
        item = dict(model='asset.netable')
        try:
            service.update(item)
        except JSONRPCException as e:
            self.assertEqual(e.error['faultCode'], 4001)
            self.assertTrue(e.error['faultString'].find('missing') > -1)

    def test_post_duplicated(self):
        service = self._get_object_service('net.dot1q.dev10b')
        item = dict(model='asset.netable',
                    value=dict(fqdn='dev10.dot1q.net',
                               ip4='192.168.123.10',
                               serialno='DRS010'))

        try:
            service.update(item)
        except JSONRPCException as e:
            self.assertEqual(e.error['faultCode'], 5001)
            self.assertTrue(e.error['faultString'].find('duplicate key') > -1)


if __name__ == '__main__':
    unittest.main()
