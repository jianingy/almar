#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : __init__.py
# created at : 2013-01-11 20:30:45
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from twisted.internet.defer import Deferred, maybeDeferred
from twisted.web import resource
from twisted.internet import reactor

from almar.service.jsonrpc import OperationService, ObjectJSONRPCProxyService
from almar.service.rest import ObjectRESTProxyService
from almar.service.proxy import AlmarProxyService

__all__ = ['worker_root', 'proxy_root']


class AlmarRootResource(resource.Resource):

    def __init__(self, max_request):
        self.request_left = max_request
        self.running = 0
        return resource.Resource.__init__(self)

    def getChild(self, path, request):
        self.running  = self.running + 1
        request.notifyFinish().addCallback(self._count, request)
        return resource.Resource.getChild(self, path, request)

    def getChildWithDefault(self, path, request):
        self.running  = self.running + 1
        request.notifyFinish().addCallback(self._count, request)
        return resource.Resource.getChildWithDefault(self, path, request)

    def _exit(self, err):
        #from sys import exit
        if self.running == 0 and reactor.running:
            reactor.stop()

    def _count(self, result, request):
        self.running = self.running - 1

        if self.request_left == 0:
            d = maybeDeferred(self.port_instance.stopListening)
            d.addBoth(self._exit)
        else:
            self.request_left = self.request_left - 1


worker_root = resource.Resource()
worker_root.putChild('op', OperationService())
worker_root.putChild('object', ObjectJSONRPCProxyService())

from almar.service.alidns import alidns_root
proxy_root = resource.Resource()
proxy_root.putChild('op', AlmarProxyService())
proxy_root.putChild('alidns', alidns_root)
