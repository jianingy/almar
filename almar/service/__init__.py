#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : __init__.py
# created at : 2013-01-11 20:30:45
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from almar.service.jsonrpc import OperationService, ObjectProxyService
from twisted.web import resource


__all__ = ['site_root']

site_root = resource.Resource()
site_root.putChild('op', OperationService())
site_root.putChild('object', ObjectProxyService())
