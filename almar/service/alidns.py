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
from collections import defaultdict
from almar.debug import debug_out
from datetime import datetime


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


@defer.inlineCallbacks
def proxy_upsert(lst):
    splitted = defaultdict(list)
    defers = list()
    g = GlobalConfig()

    for item in lst:
        range_ = find_searcher_by_path(item['path'])
        splitted[range_].append(item)

    for range_, searcher in g.searcher.iteritems():
        url = searcher['url']
        p = RPCProxy(path_join(url, 'op'))
        if splitted[range_]:
            defers.append(p.callRemote('upsert', splitted[range_]))

    try:
        writer_result = yield defer.DeferredList(defers)
        affected = filter(lambda x: x[0], writer_result)
        result = reduce(lambda x, y: x + y, map(
            lambda x: x[1]['affected'], affected), 0)

    except Exception as e:
        raise e

    defer.returnValue(result)


def fqdn(name):
    return name if name.endswith('.') else '%s.' % name


def path_reverse(path=None):
    if path:
        rpath_ = path.rstrip('.').split('.')
        rpath_.reverse()
        rpath_ = '.'.join(rpath_)
        return rpath_
    return ''


#hash: copied from proxy.py::AlmarProxyService
def path_hash(s):
    # sdbm's hash function
    hash_id = reduce(lambda hash_, c:
                     (hash_ << 6) + (hash_ << 16) - hash_ + ord(c), s, 0) % 128
    return hash_id


def find_searcher_by_path(path):
    # XXX: find a good hash function
    hash_id = path_hash(path)
    g = GlobalConfig()
    for range_, searcher in g.searcher.iteritems():
        if hash_id in range_:
            return range_

#end hash.copy


def new_soa(mname='ns1.taobao.com',
            rname='opsdns@alibaba-inc.com',
            oldserial=None,
            refresh='1800',
            retry='600',
            expire='1814400',
            minimum='300'):

    #serial <= 2^32(4294,9672,96), YYYY-MM-DD-(NR++)
    serial_ = int('%s%s' % (datetime.now().strftime('%Y%02m%02d'), '01'))
    if oldserial and (int(oldserial) > serial_):
        serial_ = '%d' % (int(oldserial) + 1)

    soa_ = dict(MNAME=mname,
                RNAME=rname,
                SERIAL=serial_,
                REFRESH=refresh,
                RETRY=retry,
                EXPIRE=expire,
                MINIMUM=minimum)

    soa = '%(MNAME)s %(RNAME)s %(SERIAL)s %(REFRESH)s %(RETRY)s %(EXPIRE)s %(MINIMUM)s' % soa_
    return soa


class DomainListService(RESTService):

    @defer.inlineCallbacks
    def async_GET(self, request):
        domains = yield search_all('__path__ ~ *.SOA.0')
        result = map(lambda x: dict(name=x['zone'], status="GoodZone"),
                     domains)
        defer.returnValue(RESTResult(code=200, content=dict(domains=result)))


class DomainService(RESTService):

    def __init__(self, domain):
        RESTService.__init__(self)
        self.domain = domain

    @defer.inlineCallbacks
    def async_GET(self, request):

        def _format(domain, rtype):
            return {'data': domain['answer'],
                    'ttl': domain['ttl'],
                    'type': rtype,
                    'name': domain['name']}

        rtype = request.args.get('rtype', ['A'])[0]
        q = '__path__ ~ *.%s.0 AND zone == %s' % (rtype.upper(), self.domain)
        domains = yield search_all(q)
        result = map(lambda x: _format(x, rtype=rtype), domains)
        defer.returnValue(RESTResult(
            code=200,
            content=dict(resourceRecords=result)))

    @defer.inlineCallbacks
    def async_POST(self, request):
        ttl_ = self.postdata.get('ttl', '3600')
        try:
            if int(ttl_) < 3600:
                ttl_ = 3600
        except Exception as e:
            debug_out('Exception: %s', str(e))
            ttl_ = 3600

        email_ = self.postdata.get(
            'emailAddress', 'dns@alibaba-inc.com').replace('@', '.')
        #TODO email.regex-match

        path_ = path_reverse(self.domain) + '.__data__.SOA.0'
        q = '__path__ == %s AND zone == %s' % (path_, self.domain)
        result = yield search_all(q)
        if len(result) > 0:
            defer.returnValue(RESTResult(code=400, content=dict(
                status='fail',
                reason='zone exist.')))

        soa = new_soa(rname=email_, expire=ttl_)
        rrdata = dict(ttl=ttl_,
                      hc=0,
                      name=self.domain,
                      type='SOA',
                      zone=self.domain,
                      ratio='1',
                      region='default',
                      content=soa,
                      answer=soa)

        data_ = dict(path=path_, model='domain', value=rrdata)
        result = yield proxy_upsert([data_])
        defer.returnValue(RESTResult(code=201, content=dict(affected=result)))

    @defer.inlineCallbacks
    def async_DELETE(self, request):
        path_ = path_reverse(self.domain)
        q = '__path__ <@ %s.__data__ AND zone == %s' % (path_, self.domain)
        #q = '__path__ ~ %s.__data__.* AND zone == %s' % (path_, self.domain)
        result = yield search_all(q)
        if len(result) > 1:
            defer.returnValue(RESTResult(code=400, content=dict(
                status='fail',
                reason='zone contains RR cannot be deleted.')))

        zpath_ = path_ + '.__data__.SOA.0'
        defers = list()
        g = GlobalConfig()
        #INFO: send DELETE to all searchers
        for range_, searcher in g.searcher.iteritems():
            url = searcher['url']
            p = RPCProxy(path_join(url, 'op'))
            defers.append(p.callRemote('delete', [zpath_]))

        try:
            rlist = yield defer.DeferredList(defers)
            rlist = filter(lambda x: x[0], rlist)
            result = reduce(lambda x, y: x+y,
                            map(lambda x: x[1]['affected'], rlist), 0)

        except Exception as e:
            raise(e)
        #NOTE: code204'll cause returned json disappeared. Find why.
        #defer.returnValue(RESTResult(code=204, content=dict(affected=result)))
        defer.returnValue(RESTResult(code=200, content=dict(affected=result)))


class RecordService(RESTService):

    isLeaf = True

    def __init__(self, domain):
        RESTService.__init__(self)
        self.domain = domain

    @defer.inlineCallbacks
    def async_GET(self, request):

        def _format(domain):
            return {'data': domain['answer'],
                    'ttl': domain['ttl'],
                    'type': domain['type'],
                    'name': domain['name']}

        _rpath = path_reverse(self.domain)
        q = '__path__ <@ %s AND zone == %s' % (_rpath, self.domain)
        domains = yield search_all(q)
        result = map(lambda x: _format(x), domains)
        defer.returnValue(RESTResult(
            code=200,
            content=dict(resourceRecords=result)))

    @defer.inlineCallbacks
    def async_POST(self, request):
        debug_out(self.postdata)

        lst_cname = list()
        lst_common = list()
        print lst_cname

        def _filter_cname():
            pass

        #1. no soa
        #2. check cname
        # zone, minTTL
        #
        result = yield proxy_upsert(lst_common)
        defer.returnValue(RESTResult(code=200, content=dict(affected=result)))

    @defer.inlineCallbacks
    def async_PUT(self, request):
        pass

    @defer.inlineCallbacks
    def async_DELETE(self, request):
        #TODO SMART_DELET support in backend
        pass


class DomainProxyService(resource.Resource):

    isLeaf = False

    def getChild(self, name, request):
        domain = request.prepath[-1]

        baselen = len(request.prepath)

        if domain:
            splitpath = request.path.strip('/').split('/')
            pathlen = len(splitpath)

            if pathlen == (baselen + 1):
                if splitpath[-1] == 'records':
                    domain = splitpath[-2]
                    return RecordService(fqdn(domain))
                ##else:
                ##    return 400.BadRequest
            else:
                return DomainService(fqdn(domain))
        else:
            return DomainListService()


class ViewProxyService(resource.Resource):

    isLeaf = False

    #TODO
    def getChild(self, name, request):
        pass


alidns_root = resource.Resource()
v1_root = resource.Resource()

alidns_root.putChild('v1', v1_root)
v1_root.putChild('domains', DomainProxyService())
v1_root.putChild('views', ViewProxyService())
