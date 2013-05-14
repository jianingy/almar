#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : plugin.py
# created at : 2012-12-20 15:29:38
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from zope.interface import implements
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.application import internet


class AlmarServiceOptions(usage.Options):
    optParameters = [
        ["config", "c", "etc/production.yaml", "Path to top configuration."],
        ["port", "p", None, "Port to listen."],
    ]

    optFlags = [
        ["proxy", "x", "Start in proxy mode"],
    ]


class AlmarServiceMaker(object):
    implements(IServiceMaker, IPlugin)

    tapname = "almar"
    description = "almar web service"
    options = AlmarServiceOptions

    def makeService(self, options):
        from almar import _init as almar_init

        # get site and default port
        port_no, site = almar_init(options['config'],
                                   mode=('normal', 'proxy')[options['proxy']])

        # override port use user specified option
        if 'port' in options and options['port']:
            port_no = int(options['port'])

        # start database connection
        if options['proxy'] == 'normal':
            from almar.backend.postgresql import PostgreSQLBackend as Backend
            b = Backend()
            b.start()

        return internet.TCPServer(port_no, site)

serviceMaker = AlmarServiceMaker()
