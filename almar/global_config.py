#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : global_config.py
# created at : 2013-01-09 11:07:28
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'


__all__ = ['GlobalConfig']

from yaml import load as yaml_load
from debug import pretty_out
from collections import namedtuple

MODE_PROXY = 1
MODE_WORKER = 2

DatabaseConfig = namedtuple('DatabaseConfig', ['host',
                                               'port',
                                               'user',
                                               'password',
                                               'dbname',
                                               'schema',
                                               'table',
                                               'min_connections',
                                               'max_connections'])

ServerConfig = namedtuple('ServerConfig', ['port', 'max_threads', 'role'])
ProxyConfig = namedtuple('ServerConfig', ['port', 'max_threads', 'writer', 'reader'])

ModelConfig = namedtuple('ModelConfig', ['model', 'member'])

MemberConfig = namedtuple('MemberConfig', ['name', 'datatype', 'flag'])


class GlobalConfig(object):

    __slots__ = ['database', 'server', 'model', 'server_mode']
    __instance__ = None

    def __new__(cls, *args, **kw):
        if not cls.__instance__:
            cls.__instance__ = super(GlobalConfig, cls).__new__(
                cls, *args, **kw)
        return cls.__instance__

    @staticmethod
    def create_instance(filename):
        from os.path import expanduser

        G = GlobalConfig
        g = G()

        with open(expanduser(filename)) as yaml:
            bc = yaml_load(yaml.read())

        G.database = G._build_sub_config(DatabaseConfig, bc['database'])
        G.server = G._build_sub_config(ServerConfig, bc['server'])

        if 'proxy' in bc:
            G.proxy = G._build_sub_config(ProxyConfig, bc['proxy'])

        models = reduce(lambda x, y: x.update(y) or x,
                        map(lambda x: dict(G._build_model_config(x)),
                            bc['model']))

        G.model = dict(map(lambda x: G._merge_inherited_member(x, models),
                           models.keys()))

        return g

    @staticmethod
    def _build_sub_config(namedtup, cfg):
        filtered = filter(lambda x: x[0] in namedtup._fields, cfg.iteritems())
        missing = map(lambda x: (x, ''),
                      filter(lambda x: x not in cfg, namedtup._fields))
        params = dict(filtered)
        params.update(dict(missing))
        return namedtup(**(params))

    @staticmethod
    def _build_model_config(filename):
        from os.path import expanduser

        G = GlobalConfig

        with open(expanduser(filename)) as yaml:
            cfg = yaml_load(yaml.read())

        for model in cfg:
            model.setdefault('member', dict())
            member = [(m['name'], G._build_sub_config(MemberConfig, m))
                      for m in model['member']]
            yield (model['model'],
                   dict(model=model['model'], member=dict(member)))

    @staticmethod
    def _merge_inherited_member(name, models):
        from copy import deepcopy

        splitted_path = name.split('.')

        if len(splitted_path) < 2:
            return (name, models[name])

        parents = ['']
        reduce(lambda x, y: x.append((".".join((x[-1], y))).lstrip('.')) or x,
               splitted_path, parents)

        model = deepcopy(models[name])

        reduce(lambda x, y: x['member'].update(models[y]['member']) or x,
               parents[1:], model)

        return (model['model'], model)

if __name__ == '__main__':
    base = "~/devel/almar"
    g = GlobalConfig.create_instance(base + "/etc/development.yaml")
    pretty_out(g.model)
    pretty_out(g.database)
    pretty_out(g.server)
    pretty_out(g.proxy)
