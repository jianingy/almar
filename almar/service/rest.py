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
from almar.debug import warn_out
from almar.backend.postgresql import PostgreSQLBackend as Backend
from almar import exception
from collections import namedtuple

RESTResult = namedtuple('RESTResult', ['code', 'content'])

class RESTService(resource.Resource):

    isLeaf = True

    def get_postdata(self, request):
        request.content.seek(0, 0)
        content = request.content.read()

        try:
            if content:
                data = json_decode(content)
            else:
                data = dict()
        except ValueError:
            raise exception.MalformedIncomingData('must be json')

        if not isinstance(data, dict):
            raise exception.MalformedIncomingData('must be json dict')

        return data


    def cancel(self, err, request, d):
        warn_out("Cancelling current request.")
        d.cancel()


    def finalize(self, result, request):
        from os import getpid

        request.setHeader('Content-Type', 'application/json; charset=UTF-8')

        if isinstance(result, Failure):
            request.setResponseCode(500)
            if isinstance(result.value, exception.AlmarErrorBase):
                error = dict(message=str(result.value), pid=getpid())
                response = dict(error=error)
            else:
                error = dict(data=result.getTraceback(),
                             message=str(result.value),
                             pid=getpid())
                response = dict(error=error)
        else:
            response = dict(result=result.content)
            request.setResponseCode(result.code)

        request.write(json_encode(response))
        if not request._disconnected:
            request.finish()

    @defer.inlineCallbacks
    def render_aux(self, request, f):
        self.postdata = self.get_postdata(request)
        retval = yield f(request)
        defer.returnValue(retval)

    def render_GET(self, request):
        d = self.render_aux(request, self.async_GET)
        request.notifyFinish().addErrback(self.cancel, request, d)
        d.addBoth(self.finalize, request)
        return NOT_DONE_YET

    def render_PUT(self, request):
        d = self.render_aux(request, self.async_PUT)
        request.notifyFinish().addErrback(self.cancel, request, d)
        d.addBoth(self.finalize, request)
        return NOT_DONE_YET

    def render_POST(self, request):
        self.postdata = self.get_postdata(request)
        d = self.render_aux(request, self.async_POST)
        request.notifyFinish().addErrback(self.cancel, request, d)
        d.addBoth(self.finalize, request)
        return NOT_DONE_YET

    def render_DELETE(self, request):
        self.postdata = self.get_postdata(request)
        d = self.render_aux(request, self.async_DELETE)
        request.notifyFinish().addErrback(self.cancel, request, d)
        d.addBoth(self.finalize, request)
        return NOT_DONE_YET


class ObjectRESTService(RESTService):

    def __init__(self, path, method):
        resource.Resource.__init__(self)
        self.path = path
        self.method = method

    @defer.inlineCallbacks
    def async_GET(self, request):
        b = Backend()
        result = yield b.get([self.path], self.method)
        if self.method == 'descendant':
            retval = RESTResult(code=200, content=result)
        else:
            retval = RESTResult(code=200, content=result[0][1])

        defer.returnValue(retval)

    @defer.inlineCallbacks
    def async_POST(self, request):
        b = Backend()

        self.postdata['path'] = self.path
        result = yield b.upsert([self.postdata])
        defer.returnValue(RESTResult(code=200, content=result))

    @defer.inlineCallbacks
    def async_DELETE(self, request):
        b = Backend()
        result = yield b.delete([self.path], False)
        defer.returnValue(result)


class ObjectRESTProxyService(resource.Resource):

    isLeaf = False

    def getChild(self, name, request):
        from os.path import splitext
        path, method = splitext(request.path[len('/rest/'):])
        path = path.replace('/', '.')
        if method.startswith('.'):
            method = method[1:].lower()
        else:
            method = 'self'

        return ObjectRESTService(path.strip('.'), method)
