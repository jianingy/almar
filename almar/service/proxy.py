#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : proxy.py
# created at : 2013-01-22 10:02:58
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from twisted.internet import defer
from txjsonrpc.web import jsonrpc
from txjsonrpc.web.jsonrpc import Proxy as RPCProxy
from txjsonrpc.jsonrpclib import Fault
from almar.global_config import GlobalConfig
from os.path import join as path_join
from collections import defaultdict

from almar.debug import out


class AlmarProxyService(jsonrpc.JSONRPC):

    addSlash = True

    def path_hash(self, s):
        # sdbm's hash function
        hash_id = reduce(lambda hash_, c: (hash_ << 6) + (hash_ << 16) - hash_ + ord(c), s, 0) % 128
        return hash_id

    def find_searcher_by_path(self, path):
        # XXX: find a good hash function
        hash_id = self.path_hash(path)
        g = GlobalConfig()
        for range_, searcher in g.searcher.iteritems():
            if hash_id in range_:
                return range_

    def jsonrpc_echo(self, s):
        return str(s)

    def jsonrpc_show(self):
        from almar.global_config import MODE_PROXY
        g = GlobalConfig()
        return dict(mode=MODE_PROXY, rc=g.proxy._asdict())

    @defer.inlineCallbacks
    def jsonrpc_search(self, query):
        g = GlobalConfig()
        result = list()
        defers = list()
        for range_, searcher in g.searcher.iteritems():
            url = searcher['url']
            out('searcher: ' + url)
            p = RPCProxy(path_join(url, 'op'))
            defers.append(p.callRemote('search', query))

        try:
            reader_result = yield defer.DeferredList(defers)
            succeed = filter(lambda x: x[0], reader_result)
            map(lambda x: result.extend(x[1]), succeed)
        except Exception as e:
            raise e

        defer.returnValue(result)

    @defer.inlineCallbacks
    def jsonrpc_upsert(self, lst):
        g = GlobalConfig()
        splitted = defaultdict(list)
        defers = list()

        for item in lst:
            range_ = self.find_searcher_by_path(item['path'])
            splitted[range_].append(item)

        for range_, searcher in g.searcher.iteritems():
            url = g.searcher[range_]['url']
            p = RPCProxy(path_join(url, 'op'))
            if splitted[range_]:
                defers.append(p.callRemote('upsert', splitted[range_]))

        try:
            writer_result = yield defer.DeferredList(defers)
            affected = filter(lambda x: x[0], writer_result)
            result = reduce(lambda x, y: x + y,
                            map(lambda x: x[1]['affected'], affected), 0)
        except Exception as e:
            raise e

        defer.returnValue(result)
