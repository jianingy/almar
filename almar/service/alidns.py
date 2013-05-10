#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : alidns.py
# created at : 2013-05-10 15:44:10
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'


from almar.service.rest import RESTService, RESTResult
from twisted.internet import defer
from twisted.internet.defer import Deferred, maybeDeferred


class AliDNSService(RESTService):

    def hello_world(self):
        return "hello, world"

    @defer.inlineCallbacks
    def async_GET(self, request):
        message = yield maybeDeferred(self.hello_world)
        retval = RESTResult(code=200, content=dict(message=message))
        defer.returnValue(retval)
