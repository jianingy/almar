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


class AlmarProxyService(jsonrpc.JSONRPC):

    addSlash = True

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
        for reader in g.proxy.reader:
            p = RPCProxy(path_join(reader, 'op'))
            defers.append(p.callRemote('search', query))

        try:
            reader_result = yield defer.DeferredList(defers)
            succeed = filter(lambda x: x[0], reader_result)
            map(lambda x: result.extend(x[1]), succeed)
        except Exception as e:
            raise e

        defer.returnValue(result)
