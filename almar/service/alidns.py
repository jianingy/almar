#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : alidns.py
# created at : 2013-05-10 15:44:10
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'


from almar.service.rest import RESTService, RESTResult
from twisted.internet import defer
from twisted.web import resource
from almar.global_config import GlobalConfig
from txjsonrpc.web.jsonrpc import Proxy as RPCProxy
from os.path import join as path_join


@defer.inlineCallbacks
def search_all(q):
    result, defers = list(), list()
    for range_, searcher in GlobalConfig().searcher.iteritems():
        url = searcher['url']
        p = RPCProxy(path_join(url, 'op'))
        defers.append(p.callRemote('search', q))

    try:
        reader_result = yield defer.DeferredList(defers)
        succeed = filter(lambda x: x[0], reader_result)
        map(lambda x: result.extend(x[1]), succeed)
    except Exception as e:
        raise e

    defer.returnValue(result)


class DomainListService(RESTService):

    @defer.inlineCallbacks
    def async_GET(self, request):
        domains = yield search_all('__path__ ~ *.SOA.0')
        result = map(lambda x: dict(name=x['zone'], status="GoodZone"),
                     domains)
        defer.returnValue(RESTResult(code=200, content=dict(domains=result)))


class DomainSearchService(RESTService):

    def __init__(self, domain):
        RESTService.__init__(self)
        self.domain = domain

    @defer.inlineCallbacks
    def async_GET(self, request):

        def _format(domain, rtype):
            return {'data': domain['answer'],
                    'ttl': domain['ttl'],
                    'type': rtype,
                    'name': ''}         # FIXME: add NAME field to database

        rtype = request.args.get('rtype', ['A'])[0]
        q = '__path__ ~ *.%s.0 AND zone == %s' % (rtype.upper(), self.domain)
        domains = yield search_all(q)
        result = map(lambda x: _format(x, rtype=rtype), domains)
        defer.returnValue(RESTResult(code=200, content=dict(domains=result)))


class DomainProxyService(resource.Resource):

    isLeaf = False

    def getChild(self, name, request):
        domain = request.prepath[-1]
        if domain:
            return DomainSearchService(domain)
        else:
            return DomainListService()


alidns_root = resource.Resource()
v1_root = resource.Resource()

alidns_root.putChild('v1', v1_root)
v1_root.putChild('domains', DomainProxyService())
