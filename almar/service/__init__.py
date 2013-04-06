#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : __init__.py
# created at : 2013-01-11 20:30:45
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

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

    def set_instance(self, port):
        self.port_instance = port

    def _count(self, result, request):
        self.running = self.running - 1

        if self.request_left == 0:
            self.port_instance.stopListening()
            if self.running == 0:
                # XXX: find a graceful way to exit.
                #      directly stop cause connections lose.
                reactor.stop()
        else:
            self.request_left = self.request_left - 1

worker_root = AlmarRootResource(5)
worker_root.putChild('op', OperationService())
worker_root.putChild('rest', ObjectRESTProxyService())
worker_root.putChild('object', ObjectJSONRPCProxyService())

proxy_root = AlmarRootResource(5)
proxy_root.putChild('op', AlmarProxyService())
