#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : test_object_delete_api.py
# created at : 2013-01-11 14:25:07
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

ALMAR_SERVER = "http://localhost:9999/"

import unittest
from jsonrpc import ServiceProxy, JSONRPCException
from copy import deepcopy

SERVICE_BASE = 'http://localhost:9999/object'


class ObjectDeleteTestCase(unittest.TestCase):

    service = None

    @staticmethod
    def _get_object_service(path):
        from os.path import join
        service_uri = join(SERVICE_BASE, path.strip('.').replace('.', '/'))
        return ServiceProxy(service_uri)

    def setUp(self):

        service = ServiceProxy('http://localhost:9999/op')

        items = [dict(path='net.dot1q.dev21',
                      model='asset.netable',
                      value=dict(fqdn='dev21.dot1q.net',
                                 ip4='192.168.123.21',
                                 serialno='DRS021')),
                 dict(path='net.dot1q.dev22',
                      model='asset.netable',
                      value=dict(fqdn='dev22.dot1q.net',
                                 ip4='192.168.123.22',
                                 serialno='DRS022')),
                 dict(path='net.dot1q.dev23',
                      model='asset.netable',
                      value=dict(fqdn='dev23.dot1q.net',
                                 ip4='192.168.123.23',
                                 serialno='DRS023'))]

        service.upsert(items)

    def test_delete_one(self):
        service = self._get_object_service('net.dot1q.dev22')
        resp = service.delete()
        self.assertIn('affected', resp)
        self.assertEqual(resp['affected'], 1)
        self.assertEqual(service.get(), dict())

    def test_delete_cascade(self):
        service = self._get_object_service('net.dot1q')
        resp = service.delete(True)
        self.assertIn('affected', resp)
        self.assertTrue(resp['affected'] > 2)
        service = self._get_object_service('net.dot1q.dev21')
        self.assertEqual(service.get(), dict())
        service = self._get_object_service('net.dot1q.dev22')
        self.assertEqual(service.get(), dict())
        service = self._get_object_service('net.dot1q.dev23')
        self.assertEqual(service.get(), dict())


if __name__ == '__main__':
    unittest.main()
