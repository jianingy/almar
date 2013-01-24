#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : proxy.py
# created at : 2013-01-22 10:02:58
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from twisted.internet import defer
from txjsonrpc.web import jsonrpc
from txjsonrpc.netstring.jsonrpc import Proxy as RPCProxy
from txjsonrpc.jsonrpclib import Fault
from almar.global_config import GlobalConfig


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
        p = RPCProxy(url, 9999)
        yield p.callRemote('search', query)
        defer.returnValue("")
