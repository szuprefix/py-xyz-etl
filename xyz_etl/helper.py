# -*- coding:utf-8 -*-
from collections import OrderedDict

__author__ = 'denishuang'

from django.db import connections
from . import models
from xyz_util import dbutils
from datetime import datetime
import logging

log = logging.getLogger("django")


def get_intervals(now=None):
    now = now or datetime.now()
    ints = []
    m = now.minute
    h = now.hour
    for a in [2, 5, 10, 20]:
        if m % a == 0:
            ints.append('%sm' % a)
    if m == 0:
        for a in [1, 2, 4]:
            if h % a == 0:
                ints.append('%sh' % a)
        for a in [1, 2, 4, 6, 19, 21]:
            if h == a:
                ints.append('1d@h%d' % a)
    return ints


def init_source():
    for c in connections.all():
        n = c.alias
        # if n == 'default':
        #     continue
        source, created = models.Source.objects.update_or_create(
            name=n,
            defaults=dict(
                type=c.vendor,
                host=c.settings_dict.get("HOST")
            ))
        source.check_is_slave()
        # init_table(source)


# def source_execute_sql(source, sql):
#     conn = connections[source.name]
#     cur = conn.cursor()
#     cur.execute(sql)


def init_table(source, table_name_filter=None):
    tns = []
    for tn in source.detect_tables(table_name_filter=table_name_filter):
        try:
            table, created = source.tables.update_or_create(
                name=tn
            )
            table.meta = table.detect_meta()
            table.save()
            tns.append(tn)
        except Exception, e:
            log.warning("etl.init_table %s.%s error: %s", source, tn, e)
    return tns


def init_field(connection, table, rows):
    fields = dbutils.get_table_fields(connection, table.name)
    for name, field in fields.items():
        try:
            table.fields.update_or_create(
                name=name,
                defaults=dict(
                    type=field.get("type"),
                    options=field
                )
            )
        except Exception, e:
            log.error("etl.init_field error: %s, %s: %s", table, name, e)


def gen_dictionary_fields_phrase(d):
    source = d.source
    conn = connections[source.name]
    cur = conn.cursor()
    cur.execute(d.sql)
    dr = {}
    for r in cur.fetchall():
        fn, fvn, fk, fv = r
        dr.setdefault(fn, {'name': fn, 'verbose_name': fvn, 'cases': {}})
        if fk is None or fv is None:
            continue
        dr[fn]['cases'][fk] = fv
    return dr


def read_transport(t):
    source = t.source
    conn = connections[source.name]
    cur = conn.cursor()
    cur.execute(t.sql_actual)
    return cur


def get_sqlalchemy_dtype(dtype, engine="mysql"):
    from sqlalchemy.dialects import postgresql, mysql
    d = {
        'mysql': {
            "str4": mysql.VARCHAR(4),
            "str8": mysql.VARCHAR(8),
            "str16": mysql.VARCHAR(16),
            "str32": mysql.VARCHAR(32),
            "str64": mysql.VARCHAR(64),
            "str128": mysql.VARCHAR(128),
            "str256": mysql.VARCHAR(256),
            "int": mysql.INTEGER
        },
        'postgresql': {
            "str4": postgresql.VARCHAR(4),
            "str8": postgresql.VARCHAR(8),
            "str16": postgresql.VARCHAR(16),
            "str32": postgresql.VARCHAR(32),
            "str64": postgresql.VARCHAR(64),
            "str128": postgresql.VARCHAR(128),
            "str256": postgresql.VARCHAR(256),
            "int": postgresql.INTEGER
        }
    }
    return d.get(engine, {}).get(dtype)
