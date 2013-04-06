#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : almarctl.py
# created at : 2013-04-04 21:27:12
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from twisted.internet import epollreactor as current_reactor
current_reactor.install()

from os import environ
from sys import argv as ARGV, executable, exit
from socket import AF_INET
from copy import deepcopy

from twisted.internet import reactor
from twisted.internet import protocol
from twisted.python import usage

from almar import _init as almar_init
from almar.debug import out


class ProcessProtocol(protocol.ProcessProtocol):

    def __init__(self, environ_, args):
        self.environ = deepcopy(environ_)
        self.worker_id = self.environ['ALMAR_WORKER_ID']
        self.arguments = args

    def connectionMade(self):
        from almar.global_config import GlobalConfig, MODE_PROXY
        g = GlobalConfig()
        mode = ('worker', 'proxy')[g.server_mode == MODE_PROXY]
        out("worker #%s started in mode %s" % (self.worker_id, mode))

    def processEnded(self, status):
        print "worker %s exited. restarting is in progress" % (self.worker_id)
        socket_fd = int(self.environ['ALMAR_SOCKET_FD'])
        fds = {0: 0, 1: 1, 2: 2, socket_fd: socket_fd}

        environ['ALMAR_SOCKET_FD'] = str(socket_fd)
        environ['ALMAR_WORKER_ID'] = str(self.worker_id)

        reactor.spawnProcess(ProcessProtocol(environ, self.arguments),
                             executable,
                             self.arguments,
                             childFDs=fds,
                             env=environ)


class Options(usage.Options):
    optParameters = [
        ['config',    'c', 'etc/production.yaml',   'path to configuration.'],
        ['port',      'p', None,                    'listening port'],
        ['process',   'n', '4',                     '# of processes'],
    ]

    optFlags = [
        ["proxy", "x", "Start in proxy mode"],
    ]


def main(options):

    # get site and default port
    port_no, site = almar_init(options['config'],
                               mode=('normal', 'proxy')[options['proxy']])

    # override port use user specified option
    if options['port']:
        port_no = int(options['port'])

    if 'ALMAR_SOCKET_FD' not in environ or environ['ALMAR_SOCKET_FD'] is None:
        # Create a new listening port and several other processes to help out.

        from procname import setprocname
        setprocname('almar master %-100s' % '')

        port = reactor.listenTCP(port_no, site)
        for i in range(int(options['process'])):
            environ['ALMAR_SOCKET_FD'] = str(port.fileno())
            environ['ALMAR_WORKER_ID'] = str(i)
            new_args = [executable]
            new_args.extend(ARGV)
            fds = {0: 0, 1: 1, 2: 2, port.fileno(): port.fileno()}
            reactor.spawnProcess(ProcessProtocol(environ, new_args),
                                 executable,
                                 new_args,
                                 childFDs=fds,
                                 env=environ)

        # master won't accept clients
        reactor.removeReader(port)
    else:
        worker_id = int(environ['ALMAR_WORKER_ID'])
        # Another process created the port, just start listening on it.
        from procname import setprocname
        setprocname('almar worker #%s%-100s' % (environ['ALMAR_WORKER_ID'], ''))

        fd = int(environ['ALMAR_SOCKET_FD'])
        port = reactor.adoptStreamPort(fd, AF_INET, site)
        site.resource.set_instance(port)

        # start database connection
        from almar.backend.postgresql import PostgreSQLBackend as Backend
        b = Backend()
        b.start(initdb=(False, True)[worker_id == 0])

    reactor.run()
    exit(0)


if __name__ == '__main__':
    options = Options()
    options.parseOptions()
    main(options)
