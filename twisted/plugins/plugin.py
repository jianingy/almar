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

from almar.util import banner


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

        from almar.global_config import GlobalConfig
        g = GlobalConfig.create_instance(options['config'])

        # configure reactor
        from twisted.internet import reactor
        reactor.suggestThreadPoolSize(int(g.server.max_threads))

        # configure database
        from txpostgres import txpostgres
        txpostgres.ConnectionPool.min = int(g.database.min_connections)
        txpostgres.ConnectionPool.max = int(g.database.max_connections)

        from almar.backend.postgresql import PostgreSQLBackend as Backend
        b = Backend.create_instance(g.database)
        b.start()

        # configure web service
        from almar.service import worker_root, proxy_root
        from twisted.web import server

        if options['proxy']:
            banner("RUNNING IN PROXY MODE")
            site = server.Site(proxy_root)
            port = int(options["port"] or g.proxy.port)
        else:
            banner("RUNNING IN WORKER MODE")
            site = server.Site(worker_root)
            port = int(options["port"] or g.server.port)

        return internet.TCPServer(port, site)


serviceMaker = AlmarServiceMaker()
