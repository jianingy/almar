#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : test_rest.py
# created at : 2013-01-11 14:25:07
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

ALMAR_SERVER = "http://localhost:9999/"

import unittest
from json import dumps as json_encode, loads as json_decode


def request(path, content=None, method='GET'):
    from os.path import join
    from urllib2 import urlopen, HTTPError

    request_url = join(ALMAR_SERVER, 'object', *(path.split('.')))
    try:
        if content:
            response = urlopen(request_url, content)
        else:
            response = urlopen(request_url)
        return json_decode(response.read())
    except HTTPError as e:
        return json_decode(e.read())


class RESTAPITestCase(unittest.TestCase):

    def test_get_with_trailing_slash(self):
        self.assertNotIn('error', request('a.b.c.'))

    def test_post_with_trailing_slash(self):
        value = dict(fqdn='cn2', ip4='1.2.3.4', serialno='dev01')
        item = dict(model='asset.netable', value=value)
        self.assertNotIn('error', request('server.dev01', json_encode(item)))

    def test_post_with_non_dict(self):
        resp = request('server.dev01', json_encode('abcd'))
        self.assertIn('error', resp)

    def test_post_with_malformed_json(self):
        resp = request('server.dev01', 'abcd')
        self.assertIn('error', resp)


if __name__ == '__main__':
    unittest.main()
