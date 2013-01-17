#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : __init__.py
# created at : 2012-12-20 16:07:11
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'


from twisted.internet import defer
from txjsonrpc.web import jsonrpc
from txjsonrpc.jsonrpclib import Fault
from twisted.web import resource
from almar.backend.postgresql import PostgreSQLBackend as Backend
from almar import exception


class OperationService(jsonrpc.JSONRPC):

    addSlash = True

    @defer.inlineCallbacks
    def jsonrpc_upsert(self, lst):
        try:
            b = Backend()
            result = yield b.upsert(lst)
            defer.returnValue(result)
        except exception.ModelNotExistError as e:
            defer.returnValue(Fault(exception.INVALID_INPUT, str(e)))
        except exception.MissingFieldError as e:
            defer.returnValue(Fault(exception.INVALID_INPUT, str(e)))
        except exception.ConstraintViolationError as e:
            defer.returnValue(Fault(exception.CONSTRAINT_VIOLATION, str(e)))
        except Exception as e:
            raise

    def jsonrpc_get(self, paths, method='self'):
        b = Backend()
        return b.get(paths, method)

    def jsonrpc_delete(self, paths, cascade=False):
        print paths, cascade
        b = Backend()
        return b.delete(paths, cascade)

    def jsonrpc_touch(self, paths):
        b = Backend()
        return b.touch(paths)

    @defer.inlineCallbacks
    def jsonrpc_search(self, query):
        b = Backend()
        result = yield b.search(query)
        defer.returnValue(result)


class ObjectService(jsonrpc.JSONRPC):

    addSlash = True

    def __init__(self, path):
        jsonrpc.JSONRPC.__init__(self)
        self.path = path

    @defer.inlineCallbacks
    def jsonrpc_get(self):
        try:
            b = Backend()
            resp = yield b.get([self.path], None)
            if resp:
                defer.returnValue(resp[0][1])
            else:
                defer.returnValue(dict())
        except:
            raise

    @defer.inlineCallbacks
    def jsonrpc_update(self, item):
        try:
            b = Backend()
            if isinstance(item, dict):
                item['path'] = self.path
                resp = yield b.upsert([item])
                defer.returnValue(resp)
            else:
                defer.returnValue(Fault(exception.INVALID_INPUT,
                                        "content must be json dict"))
        except exception.MissingFieldError as e:
            defer.returnValue(Fault(exception.INVALID_INPUT, str(e)))
        except exception.ModelNotExistError as e:
            defer.returnValue(Fault(exception.INVALID_INPUT, str(e)))
        except exception.ConstraintViolationError as e:
            defer.returnValue(Fault(exception.CONSTRAINT_VIOLATION, str(e)))
        except:
            raise

    @defer.inlineCallbacks
    def jsonrpc_delete(self, cascade=False):
        try:
            b = Backend()
            resp = yield b.delete([self.path], cascade)
            defer.returnValue(resp)
        except:
            raise

    @defer.inlineCallbacks
    def jsonrpc_push(self, lst):
        try:
            b = Backend()
            resp = yield b.push(self.path, lst)
            defer.returnValue(resp)
        except exception.MissingFieldError as e:
            defer.returnValue(Fault(exception.INVALID_INPUT, str(e)))
        except exception.ModelNotExistError as e:
            defer.returnValue(Fault(exception.INVALID_INPUT, str(e)))
        except exception.MalformedIncomingData as e:
            defer.returnValue(Fault(exception.INVALID_INPUT, str(e)))
        except exception.ConstraintViolationError as e:
            defer.returnValue(Fault(exception.CONSTRAINT_VIOLATION, str(e)))
        except:
            raise


class ObjectProxyService(resource.Resource):

    isLeaf = False

    def getChild(self, name, request):
        from os.path import splitext
        path, method = splitext(request.path[len('/object/'):])
        path = path.replace('/', '.')
        if method.startswith('.'):
            method = method[1:].lower()
        else:
            method = 'self'

        return ObjectService(path.strip('.'))
