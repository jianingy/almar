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
        ["config", "c", "etc/development.yaml", "Path to top configuration."],
        ["port", "p", 9999, "Port to listen."],
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
        from almar.service import site_root
        from twisted.web import server
        site = server.Site(site_root)

        return internet.TCPServer(int(options["port"] or g.srv['port']), site)


serviceMaker = AlmarServiceMaker()
