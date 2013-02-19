#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : __init__.py
# created at : 2013-01-11 20:30:45
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from almar.service.jsonrpc import OperationService
from almar.service.rest import ObjectRESTProxyService
from almar.service.proxy import AlmarProxyService
from twisted.web import resource


__all__ = ['worker_root', 'proxy_root']

worker_root = resource.Resource()
worker_root.putChild('op', OperationService())
worker_root.putChild('object', ObjectRESTProxyService())

proxy_root = resource.Resource()
proxy_root.putChild('op', AlmarProxyService())
