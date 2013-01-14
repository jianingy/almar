#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : test_rest.py
# created at : 2013-01-11 14:25:07
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

ALMAR_SERVER = "http://localhost:9999/"

import unittest
from jsonrpc import ServiceProxy
from copy import deepcopy

SERVICE_BASE = 'http://localhost:9999/object'


class ObjectGetTestCase(unittest.TestCase):

    service = None

    @staticmethod
    def _get_object_service(path):
        from os.path import join
        service_uri = join(SERVICE_BASE, path.strip('.').replace('.', '/'))
        return ServiceProxy(service_uri)

    @classmethod
    def setUpClass(cls):

        service = ServiceProxy('http://localhost:9999/op')

        items = [dict(path='net.dot1q.dev01',
                      model='asset.netable',
                      value=dict(fqdn='dev01.dot1q.net',
                                 ip4='192.168.123.1',
                                 serialno='DRS001')),
                 dict(path='net.dot1q.dev02',
                      model='asset.netable',
                      value=dict(fqdn='dev02.dot1q.net',
                                 ip4='192.168.123.2',
                                 serialno='DRS002'))]
        cls.items = items
        service.upsert(items)

    def test_get(self):
        service = self._get_object_service('net.dot1q.dev01')
        expect = deepcopy(self.items[0]['value'])
        expect['__model__'] = self.items[0]['model']
        self.assertEqual(service.get(), expect)

    def test_get_nonexist(self):
        service = self._get_object_service('net.dot1q.dev01.xxx')
        self.assertEqual(service.get(), dict())


if __name__ == '__main__':
    unittest.main()
