#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : restservice.py
# created at : 2013-01-11 20:27:48
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from twisted.internet import defer
from twisted.web import resource
from twisted.web.server import NOT_DONE_YET
from twisted.python.failure import Failure
from ujson import encode as json_encode, decode as json_decode
from debug import warn_out
from almar.backend.postgresql import PostgreSQLBackend as Backend
from almar import exception


class ObjectRESTService(resource.Resource):

    isLeaf = True

    def __init__(self, path, method):
        resource.Resource.__init__(self)
        self.path = path
        self.method = method

    def cancel(self, err, d):
        warn_out("Cancelling current request.")
        d.cancel()

    def finalize(self, value, request):

        request.setHeader('Content-Type', 'application/json; charset=UTF-8')

        if isinstance(value, Failure):
            request.setResponseCode(500)
            if isinstance(value.value, AlmarError.AlmarErrorBase):
                error = dict(message=str(value.value))
                response = dict(error=error)
            else:
                error = dict(data=value.getTraceback(),
                             message=str(value.value))
                response = dict(error=error)
        else:
            response = dict(result=value)
            request.setResponseCode(200)

        request.write(json_encode(response))
        request.finish()

    def render_GET(self, request):
        d = self.async_GET(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addBoth(self.finalize, request)
        return NOT_DONE_YET

    def render_POST(self, request):
        d = self.async_POST(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addBoth(self.finalize, request)
        return NOT_DONE_YET

    def render_DELETE(self, request):
        d = self.async_DELETE(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addBoth(self.finalize, request)
        return NOT_DONE_YET

    @defer.inlineCallbacks
    def async_GET(self, request):
        b = Backend()
        result = yield b.get([self.path], self.method)
        defer.returnValue(result[0][1])

    @defer.inlineCallbacks
    def async_POST(self, request):
        b = Backend()

        request.content.seek(0, 0)
        content = request.content.read()
        try:
            data = json_decode(content)
        except ValueError:
            raise AlmarError.MalformedIncomingData('must be json')
        if not isinstance(data, dict):
            raise AlmarError.MalformedIncomingData('must be json dict')
        data['path'] = self.path
        result = yield b.upsert([data])
        defer.returnValue(result)

    @defer.inlineCallbacks
    def async_DELETE(self, request):
        b = Backend()
        result = yield b.delete([self.path], False)
        defer.returnValue(result)
