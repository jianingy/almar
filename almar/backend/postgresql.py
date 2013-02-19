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
from almar.lex import parse as parse_query
from almar.util import quote
from almar import lex

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

    SELECT_OBJECT_MANY = ('SELECT path, (each(value)).key, (each(value)).value'
                          ' FROM (SELECT path, '
                          ' (value || (\'"__model__" => "\''
                          ' || model::TEXT || \'"\')::HSTORE) AS value'
                          ' FROM %s WHERE %s) AS values')

    GET_LAST_ID = "SELECT currval(%s)"

    GET_DESCENDANT = ('SELECT path FROM %s WHERE path <@ %%s AND path != %%s')


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
                filter(lambda x: getattr(self.g.database, x),
                    ['host', 'port', 'user', 'password', 'dbname'])))
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

    def make_node(self, x, ignores=[]):
        try:
            map(lambda ignore: x.setdefault(ignore, None), ignores)
            return AlmarNode(path=x['path'],
                             model=x['model'],
                             value=x['value'])
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
            nodes = map(lambda x: self.make_node(x, ignores=['model']), inputs)
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
        yield c.execute(SQL.GET_OBJECT_MODEL % self.s_object, [item.path])
        result = c.fetchall()
        model_name = result[0][0] if result else item.model

        # model_name will be None if input without 'model' field
        # check the code of _make_node for detail
        if model_name is None:
            raise exception.MissingFieldError(
                "the following fields are missing: model")

        try:
            model = self.g.model[model_name]
        except KeyError:
            raise exception.ModelNotExistError(
                'model %s does not exist' % model_name)

        # upsert item
        weird_keys = set(item.value.keys()).difference(model['member'].keys())
        if weird_keys:
            raise exception.KeyNotDefinedError(
                'undefined key: %s' % ",".join(weird_keys))
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
            if method == 'descendant':
                yield c.execute(SQL.GET_DESCENDANT % self.s_object,
                                [path, path])
                result.extend(map(lambda x: x[0], c.fetchall()))
            else:
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
        nodes = map(lambda x: self.make_node(x, ignores=['path']), inputs)
        result = yield self.conn.runInteraction(self._push_xaction,
                                                path, nodes)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _push_xaction(self, c, path, items):
        affected = 0

        yield c.execute(SQL.GET_OBJECT % self.s_object, [path])
        parent = dict(c.fetchall())
        next_id = int(parent['__seq__']) if '__seq__' in parent else 0

        for item in items:
            affected += yield self._push_one(c, path, next_id, item)
            next_id = next_id + 1

        hstore_value = self.serialize_hstore(dict(__seq__=next_id))
        yield c.execute(SQL.UPDATE_OBJECT % self.s_object,
                        [parent['__model__'], hstore_value, path])

        defer.returnValue(dict(affected=affected))

    @defer.inlineCallbacks
    def _push_one(self, c, path, next_id, item):

        item_path = "%s.%s" % (path, next_id)
        print 'id = ', next_id, 'value =', item.value
        hstore_value = self.serialize_hstore(item.value)
        model_name = item.model

        try:
            model = self.g.model[model_name]
        except KeyError:
            raise exception.ModelNotExistError(
                'model %s does not exist' % model_name)

        yield c.execute(SQL.CREATE_OBJECT % self.s_object,
                        [item_path, model['model'], hstore_value])

        affected = c._cursor.rowcount
        defer.returnValue(affected)

    ##########################################################################
    #
    # API: SEARCH
    #
    ##########################################################################

    @defer.inlineCallbacks
    def search(self, query):
        where_clause = self._build_where_clause(parse_query(query))
        sql = SQL.SELECT_OBJECT_MANY % (self.s_object, where_clause)
        rows = yield self.conn.runQuery(sql)
        result = dict()
        reduce(self._reduce_search_result, rows, result)

        def _merge(x):
            return dict(dict(x[1]).items() + {'__path__': x[0]}.items())

        defer.returnValue(map(_merge, result.iteritems()))

    def _reduce_search_result(self, result, x):
        result.setdefault(x[0], list())
        result[x[0]].append((x[1], x[2]))
        return result

    def _build_where_clause(self, suffix):
        # NOTICE: DO NOT ADD DEFAULT SEARCH HERE
        #         If there is only one value in the search expression,
        #         someone may ask to search it as nodename. PLEASE DO NOT
        #         ADD this feature here in order to keep the code clean !!!

        suffix.reverse()
        stack = list()

        if len(suffix) == 1:
            raise exception.SearchGrammarError("incomplete search query")

        while suffix:
            term, term_type, pos = suffix.pop()
            if term_type == lex.TOKEN_OPER:
                rhs, quoted = stack.pop()
                if not quoted:
                    rhs = quote(rhs)
                try:
                    lhs, quoted = stack.pop()
                    if not quoted:
                        lhs = quote(lhs)
                except IndexError:
                    vicinity = " ".join(map(lambda x: x[1], suffix))
                    raise exception.SearchGrammarError(
                        "missing operand near '%s' at position %s"
                        % (vicinity, pos))

                # use ILIKE instead of ~ for case-insensitive
                if term in ("~", "!~"):
                    term = ("ILIKE", "NOT ILIKE")[term == "!~"]
                    # escape special characters of LIKE expression
                    rhs.replace("%", "\%").replace("_", "\_")
                    rhs = "%%%s%%" % rhs

                if lhs.lower() in ('__model__', '__path__'):
                    lhs = lhs.strip('_')
                    if term == '==':
                        where_clause = \
                            "lower(\"%s\") = lower(E'%s')" % (lhs, rhs)
                    elif term == '!=':
                        where_clause = \
                            "lower(\"%s\") != lower(E'%s')" % (lhs, rhs)
                    elif term == '===':
                        where_clause = "\"%s\" = E'%s'" % (lhs, rhs)
                    elif term == '!==':
                        where_clause = "\"%s\" != E'%s'" % (lhs, rhs)
                    elif term == "IN":
                        _ = ",".join(map(lambda x:
                                         "lower('%s')" % quote(x),
                                         rhs.split(",")))
                        where_clause = "lower(%s) = ANY(ARRAY[%s])" % (lhs, _)
                    elif term == "^":
                        where_clause = "lower(\"%s\") LIKE lower(E'%s%%')" % (lhs, rhs)
                    else:
                        where_clause = "\"%s\" %s E'%s'" % (lhs, term, rhs)
                elif lhs.lower() in ('id'):
                    if term in ('==', '==='):
                        term = '='
                        where_clause = "\"%s\" %s E'%s'" % (lhs, term, rhs)
                    elif term in ('!=='):
                        term = '!='
                        where_clause = "\"%s\" %s E'%s'" % (lhs, term, rhs)
                    elif term == "IN":
                        _ = ",".join(map(lambda x: "%s" % quote(x),
                        rhs.split(",")))
                        where_clause = "%s = ANY(ARRAY[%s])" % (lhs, _)
                elif term == "IN":
                    _ = ",".join(map(lambda x: "lower('%s')" % quote(x),
                                     rhs.split(",")))
                    where_clause = \
                        "lower(value->E'%s') = ANY(ARRAY[%s])" % (lhs, _)
                elif term == "==":
                    where_clause = \
                        "lower(value->E'%s') = lower(E'%s')" % (lhs, rhs)
                elif term == "!=":
                    where_clause = \
                        "lower(value->E'%s') != lower(E'%s')" % (lhs, rhs)
                elif term == "===":
                    where_clause = "value->E'%s' = E'%s'" % (lhs, rhs)
                elif term == "!==":
                    where_clause = "value->E'%s' != E'%s'" % (lhs, rhs)
                elif term in ("^"):
                    term = "LIKE"
                    rhs.replace("%", "\%").replace("_", "\_")
                    rhs = "%s%%" % rhs
                    where_clause = "lower(value->E'%s') LIKE lower(E'%s')" \
                        % (lhs, rhs)
                else:
                    where_clause = "value->E'%s' %s E'%s'" % (lhs, term, rhs)

                stack.append((where_clause, True))
            elif term_type == lex.TOKEN_LOGIC:
                try:
                    rhs, quoted = stack.pop()
                    lhs, quoted = stack.pop()
                except IndexError:
                    vicinity = term
                    raise backend.SearchGrammarError(
                        "missing logic clause near '%s' at position %s"
                        % (vicinity, pos))
                where_clause = " ".join((lhs, term.upper(), rhs))
                stack.append(("(%s)" % where_clause, True))
            else:
                stack.append((term, False))

        if len(stack) == 1:
            return stack[0][0]
        else:
            raise exception.SearchGrammarError(
                "syntax error at position %s" % (pos))
