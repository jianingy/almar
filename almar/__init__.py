#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : __init__.py<2>
# created at : 2013-01-15 16:44:35
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'


def _init(config, mode='normal'):
    from almar.global_config import GlobalConfig, MODE_PROXY, MODE_WORKER
    g = GlobalConfig.create_instance(config)

    # configure reactor
    from twisted.internet import reactor
    reactor.suggestThreadPoolSize(int(g.server.max_threads))

    # configure database
    from txpostgres import txpostgres
    txpostgres.ConnectionPool.min = int(g.database.min_connections)
    txpostgres.ConnectionPool.max = int(g.database.max_connections)

    from almar.backend.postgresql import PostgreSQLBackend as Backend
    Backend.create_instance(g.database)

    # configure web service
    from almar.service import worker_root, proxy_root
    from twisted.web import server

    if mode == 'proxy':
        g.server_mode = MODE_PROXY
        return int(g.proxy.port), server.Site(proxy_root)
    else:
        g.server_mode = MODE_WORKER
        return int(g.server.port), server.Site(worker_root)
