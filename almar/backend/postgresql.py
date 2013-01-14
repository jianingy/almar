#!/usr/bin/env python
# -*- coding: utf-8 -*-

# filename   : postgresql.py<2>
# created at : 2013-01-09 14:09:23
# author     : Jianing Yang <jianingy.yang AT gmail DOT com>

__author__ = 'Jianing Yang <jianingy.yang AT gmail DOT com>'

from collections import namedtuple
from twisted.internet import defer
from txpostgres import txpostgres

from almar.global_config import GlobalConfig
from almar import exception
from almar.debug import out, pretty_out
from almar.debug import fatal_out, warn_out

import psycopg2

AlmarNode = namedtuple('AlmarNode', 'path, model, value')


class SQL(object):

    LIST_TABLE = ('SELECT schemaname, tablename, tableowner FROM pg_tables'
                  '  WHERE schemaname=%s AND tablename=%s')

    LIST_EXTENSION = ('SELECT extname FROM pg_extension WHERE extname=%s')

    LIST_SCHEMA = ('SELECT schema_name FROM information_schema.schemata'
                   ' WHERE schema_name = %s')

    LIST_INDEX = ('SELECT indexname FROM pg_indexes'
                  ' WHERE schemaname=%s AND tablename=%s')

    LIST_CONSTRAINT = ('SELECT conname FROM pg_constraint')

    CREATE_EXTENSION = ('CREATE EXTENSION %s')

    CREATE_SCHEMA = ('CREATE SCHEMA %s')

    CREATE_OBJECT_TABLE = ('CREATE TABLE %s (path LTREE PRIMARY KEY, '
                           'model LTREE, value HSTORE)')

    CREATE_OBJECT = ('INSERT INTO %s(path, model, value)'
                     ' VALUES (%%s, %%s, %%s)')

    UPDATE_OBJECT = ('UPDATE %s SET model=%%s,'
                     ' value=value || %%s WHERE path=%%s')

    GET_OBJECT = ('SELECT key, value FROM each((SELECT value ||'
                  ' (\'"__model__" => "\' || model::text || \'"\')::hstore'
                  ' FROM %s WHERE path=%%s))')

    DELETE_OBJECT = 'DELETE FROM %s WHERE path = %%s'
    DELETE_OBJECT_CASCADE = 'DELETE FROM %s WHERE path <@ %%s'

    TRY_TOUCH_OBJECT = 'UPDATE %s SET path=path WHERE path=%%s'
    TOUCH_OBJECT = 'INSERT INTO %s(path) VALUES (%%s)'

    GET_OBJECT_MODEL = 'SELECT model FROM %s WHERE path=%%s'

    GET_LAST_ID = "SELECT currval(%s)"


class PostgreSQLBackend(object):

    __instance__ = None

    def __new__(cls, *args, **kw):
        if not cls.__instance__:
            cls.__instance__ = super(PostgreSQLBackend, cls).__new__(
                cls, *args, **kw)
        return cls.__instance__

    @staticmethod
    def create_instance(cfg):
        b = PostgreSQLBackend()
        b.g = GlobalConfig()
        b.schema = b.g.database.schema
        b.t_object = b.g.database.table
        b.s_object = '.'.join([b.schema, b.t_object])

        return b

    def start(self, *args, **kwargs):
        dsn = " ".join(
            map(lambda x: "%s=%s" % (x, getattr(self.g.database, x)),
                ['host', 'port', 'user', 'password']))
        self.conn = txpostgres.ConnectionPool(None, dsn)
        d = self.conn.start()
        d.addCallback(self._init_extension)
        d.addCallback(self._init_schema)
        d.addCallback(self._init_table)
        d.addCallback(self._init_table_constraint)
        #d.addErrback(lambda x: fatal_out('database connection error'))
        return d

    ##########################################################################
    #
    # INIT ROUTINES
    #
    ##########################################################################
    @defer.inlineCallbacks
    def _init_extension(self, conn):
        # check and create missing extensions
        result = yield conn.runQuery(SQL.LIST_EXTENSION, ('ltree',))

        if not result:
            # create ltree extension
            out('creating ltree extension')
            yield conn.runOperation(SQL.CREATE_EXTENSION % 'ltree')

        result = yield conn.runQuery(SQL.LIST_EXTENSION, ('hstore',))

        if not result:
            # create ltree extension
            out('creating hstore extension')
            yield conn.runOperation(SQL.CREATE_EXTENSION % 'hstore')

        defer.returnValue(conn)

    @defer.inlineCallbacks
    def _init_schema(self, conn):
        # check and create missing schema
        result = yield conn.runQuery(SQL.LIST_SCHEMA, (self.schema,))
        if not result:
            # create ltree extension
            out('creating %s schema' % self.schema)
            yield conn.runOperation(SQL.CREATE_SCHEMA % self.schema)

        defer.returnValue(conn)

    @defer.inlineCallbacks
    def _init_table(self, conn):
        # check and create missing tables

        result = yield conn.runQuery(SQL.LIST_TABLE,
                                     (self.schema, self.t_object))
        if not result:
            # create object table
            out('creating object table %s' % self.s_object)
            yield conn.runOperation(SQL.CREATE_OBJECT_TABLE % self.s_object)

        result = yield conn.runQuery(SQL.LIST_TABLE,
                                     (self.schema, self.t_object))

        defer.returnValue(conn)

    @defer.inlineCallbacks
    def _init_table_constraint(self, c):
        # get all indice
        result = yield c.runQuery(SQL.LIST_INDEX,
                                     (self.schema, self.t_object))
        if result:
            indice = map(lambda x: x[0], result)
        else:
            indice = []

        # get all constraints
        result = yield c.runQuery(SQL.LIST_CONSTRAINT)
        if result:
            constraints = map(lambda x: x[0], result)
        else:
            constraints = []

        for model_name, model in self.g.model.iteritems():
            members = self.g.model[model_name]['member']
            for member_name, member in members.iteritems():
                if 'blank' in member.flag and not member.flag['blank']:
                    yield self._add_constraint_blank(c, model_name,
                                                     member_name, constraints)
                if 'unique' in member.flag:
                    yield self._add_constraint_unique(c, model_name,
                                                      member_name, indice,
                                                      member.flag['unique'])

    @defer.inlineCallbacks
    def _add_constraint_blank(self, c, model, member, constraints):
        cname = 'almar_check_blank_%s_%s' % (
            model.replace('_', '__').replace('.', '_'), member)
        sql = ('ALTER TABLE %s ADD CONSTRAINT %s'
               " CHECK (model!='%s'"
               " OR (model='%s'"
               " AND (value->E'%s') IS NOT NULL"
               " AND LENGTH((value->E'%s')) > 0))"
               % (self.s_object, cname, model, model, member, member))

        if cname not in constraints:
            out('creating blank constraint %s' % cname)
            yield c.runOperation(sql)

        defer.returnValue(c)

    @defer.inlineCallbacks
    def _add_constraint_unique(self, c, model, member, indice, flag):
        uname = 'almar_unique_%s_%s_%s' % (
            flag, model.replace('_', '__').replace('.', '_'), member)

        if flag == 'inherit':
            sql = ("CREATE UNIQUE INDEX %s ON %s ((value->E'%s'))"
                   " WHERE model <@ %%s" % (uname, self.s_object, member))
        else:
            sql = ("CREATE UNIQUE INDEX %s ON %s ((value->E'%s'))"
                   " WHERE model = %%s" % (uname, self.s_object, member))

        if uname not in indice:
            out('creating unique (%s) index %s' % (flag, uname))
            yield c.runOperation(sql, [model])

        defer.returnValue(c)

    ##########################################################################
    #
    # HELPER ROUTINES
    #
    ##########################################################################
    @staticmethod
    def serialize_hstore(val):
        """
        Serialize a dictionary into an hstore literal. Keys and values
        must both be strings.
        """
        esc = lambda v: unicode(v).replace('"', r'\"').encode("UTF-8")
        return ', '.join('"%s"=>"%s"' % (esc(k), esc(v))
                         for k, v in val.iteritems())

    def make_node(self, x):
        try:
            return AlmarNode(path=x['path'], model=x['model'], value=x['value'])
        except KeyError as e:
            raise exception.MissingFieldError(
                "the following fields are missing: " + ",".join(e.args))

    ##########################################################################
    #
    # API: UPSERT (UPDATE & INSERT)
    #
    ##########################################################################
    @defer.inlineCallbacks
    def upsert(self, inputs):
        try:
            nodes = map(self.make_node, inputs)
            affected = yield self.conn.runInteraction(self._upsert_xaction,
                                                      nodes)
            defer.returnValue(dict(affected=affected))
        except psycopg2.IntegrityError as e:
            if int(e.pgcode) == 23505:  # UNIQUE VIOLATION
                raise exception.ConstraintViolationError(e.message)
            else:
                raise e

    @defer.inlineCallbacks
    def _upsert_xaction(self, c, items):
        affected = 0
        for item in items:
            affected += yield self._upsert_one(c, item)

        defer.returnValue(affected)

    @defer.inlineCallbacks
    def _upsert_one(self, c, item):

        # get node model
        yield c.execute(SQL.GET_OBJECT_MODEL % self.s_object, [item.model])
        result = c.fetchall()
        model_name = result[0][0] if result else item.model
        try:
            model = self.g.model[model_name]
        except KeyError:
            raise exception.ModelNotExistError(
                'model %s does not exist' % model_name)

        # upsert item
        hstore_value = self.serialize_hstore(item.value)

        # try update first
        yield c.execute(SQL.UPDATE_OBJECT % self.s_object,
                        [model['model'], hstore_value, item.path])

        if c._cursor.rowcount < 1:
            # if nothing updated, do insert
            yield c.execute(SQL.CREATE_OBJECT % self.s_object,
                            [item.path, model['model'], hstore_value])

        affected = c._cursor.rowcount

        defer.returnValue(affected)

    ##########################################################################
    #
    # API: GET
    #
    ##########################################################################
    @defer.inlineCallbacks
    def get(self, paths, method):
        result = yield self.conn.runInteraction(self._get_xaction,
                                                paths, method)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _get_xaction(self, c, paths, method=''):
        result = []
        for path in paths:
            yield c.execute(SQL.GET_OBJECT % self.s_object, [path])
            result.append((path, dict(c.fetchall())))

        defer.returnValue(result)

    ##########################################################################
    #
    # API: DELETE
    #
    ##########################################################################
    @defer.inlineCallbacks
    def delete(self, paths, cascade):
        affected = yield self.conn.runInteraction(self._delete_xaction,
                                                  paths, cascade)
        defer.returnValue(dict(affected=affected))

    @defer.inlineCallbacks
    def _delete_xaction(self, c, paths, cascade):
        affected = 0
        for path in paths:
            if cascade:
                yield c.execute(SQL.DELETE_OBJECT_CASCADE % self.s_object,
                                [path])
            else:
                yield c.execute(SQL.DELETE_OBJECT % self.s_object, [path])

            affected = affected + c._cursor.rowcount

        defer.returnValue(affected)

    ##########################################################################
    #
    # API: TOUCH
    #
    ##########################################################################
    @defer.inlineCallbacks
    def touch(self, inputs):
        nodes = map(lambda x: AlmarNode(path=x['path'],
                                        model='',
                                        value=''), inputs)

        result = yield self.conn.runInteraction(self._touch_xaction, nodes)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _touch_xaction(self, c, items):
        affected = 0
        for item in items:
            affected += yield self._touch_one(c, item)

        defer.returnValue(affected)

    @defer.inlineCallbacks
    def _touch_one(self, c, item):

        # try touch by update first
        yield c.execute(SQL.TRY_TOUCH_OBJECT % self.s_object, [item.path])

        if c._cursor.rowcount < 1:
            # if nothing updated, do insert
            yield c.execute(SQL.TOUCH_OBJECT % self.s_object, [item.path])

        affected = c._cursor.rowcount

        defer.returnValue(affected)

    ##########################################################################
    #
    # API: PUSH
    #
    ##########################################################################
    @defer.inlineCallbacks
    def push(self, path, inputs):
        nodes = map(lambda x: AlmarNode(path=x['path'],
                                        model='',
                                        value=''), inputs)

        result = yield self.conn.runInteraction(self._push_xaction,
                                                path, nodes)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _push_xaction(self, c, path, items):
        affected = 0

        yield c.execute(SQL.GET_OBJECT % self.s_object, [path])
        print dict(c.fetchall())

        for item in items:
            affected += yield self._touch_one(c, path, item)

        defer.returnValue(affected)

    @defer.inlineCallbacks
    def _push_one(self, c, path, item):

        # try touch by update first
        yield c.execute(SQL.TRY_TOUCH_OBJECT % self.s_object, [item.path])

        if c._cursor.rowcount < 1:
            # if nothing updated, do insert
            yield c.execute(SQL.TOUCH_OBJECT % self.s_object, [item.path])

        affected = c._cursor.rowcount

        defer.returnValue(affected)
