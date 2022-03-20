# -*- coding:utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from django.utils.functional import cached_property
from xyz_util import modelutils, pandasutils, dbutils

# Create your models here.
import logging

log = logging.getLogger("django")


class Source(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '数据源'

    name = models.CharField('名称', max_length=64)
    verbose_name = models.CharField('中文名称', max_length=64)
    type = models.CharField('类型', max_length=64)
    host = models.CharField('地址', max_length=128)
    settings = modelutils.JSONField("配置", blank=True, null=True, help_text="配置内容请参见django文档中关于DATABASES的配置, 只需一个连接的内容填入此处")
    is_active = models.BooleanField('有效', default=True, blank=True)
    is_slave = models.BooleanField('是备机', default=False, blank=True)
    alchemy_str = models.CharField(
        'alchemy连接串', max_length=256, null=True, blank=True)
    schemas = modelutils.JSONField("schema列表", blank=True, default=[])

    def __unicode__(self):
        if self.name == self.verbose_name:
            return self.name
        return "%s(%s)" % (self.name, self.verbose_name)

    @cached_property
    def connection(self):
        from django.db.utils import load_backend
        backend = load_backend("django.db.backends.%s" % self.type)
        d = {}
        d.setdefault('ATOMIC_REQUESTS', False)
        d.setdefault('AUTOCOMMIT', True)
        d.setdefault('ENGINE', 'django.db.backends.dummy')
        d.setdefault('CONN_MAX_AGE', 0)
        d.setdefault('OPTIONS', {})
        d.setdefault('TIME_ZONE', None)
        d.update(self.settings or {})
        return backend.DatabaseWrapper(d, self.name)

    def query(self, sql, coerce_float=True):
        con = self.type == 'hive' and self.alchemy_str or self.connection
        return pandasutils.get_dataframe_from_table(sql, con, coerce_float=coerce_float)

    def execute(self, sql):
        cursor = self.connection.cursor()
        return cursor.execute(sql)

    def check_is_slave(self):
        try:
            self.is_slave = get_slave_time(self.name) and True or False
            self.save()
        except Exception, e:
            log.error("etl.Source %s .check_is_slave() error:%s", self, e)

    def detect_schemas(self):
        if self.type == "postgresql":
            df = self.query(
                "SELECT nspname FROM pg_namespace where nspname not like 'pg_%' and nspname not in ('information_schema','public')"
            )
            return list(df['nspname'])
        return []

    def detect_tables(self, table_name_filter=None):
        introspection = self.connection.introspection
        table_names = []
        schemas = self.detect_schemas()
        if self.schemas != schemas:
            self.schemas = schemas
            self.save()
        with self.connection.cursor() as cursor:
            for schema in [""] + schemas:
                dbutils.switch_schema(cursor, schema)
                for table_name in introspection.table_names(cursor):
                    if table_name_filter is not None and callable(
                            table_name_filter):
                        if not table_name_filter(table_name):
                            continue
                    name = "%s.%s" % (schema,
                                      table_name) if schema else table_name
                    table_names.append(name)
        return table_names

    def save(self, **kwargs):
        if not self.verbose_name:
            self.verbose_name = self.name
        if not (self.alchemy_str and self.alchemy_str.startswith("hive://")):
            self.alchemy_str = dbutils.db_sqlalchemy_str(self.connection)
        self.host = self.settings.get('HOST')
        self.type = self.settings.get('ENGINE').split('.')[-1].lower()
        return super(Source, self).save(**kwargs)


class Table(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '数据表'

    source = models.ForeignKey(
        Source, verbose_name=Source._meta.verbose_name, related_name='tables')
    name = models.CharField('名称', max_length=64)
    verbose_name = models.CharField('中文名称', max_length=64, blank=True)
    meta = modelutils.JSONField('元信息', null=True, blank=True, default={})
    is_dimession = models.BooleanField('是维度表', default=False, blank=True)
    is_fact = models.BooleanField('是事实表', default=False, blank=True)
    is_active = models.BooleanField('有效', default=True, blank=True)

    def __unicode__(self):
        return self.name

    def save(self, **kwargs):
        if not self.meta:
            self.meta = {}
        if not self.verbose_name or self.verbose_name == self.name:
            self.verbose_name = self.meta.get('comment') or self.name
        return super(Table, self).save(**kwargs)

    def count(self, cond=""):
        df = self.source.query(
            "select count(1) from %s %s" % (self.name, cond))
        return df.loc[0][0]

    def detect_meta(self):
        return dbutils.get_table_schema(self.source.connection, self.name)

    def update_meta(self):
        self.meta = self.detect_meta()
        self.save()


class Field(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '数据字段'

    source = models.ForeignKey(
        Source, verbose_name=Source._meta.verbose_name, related_name='fields')
    table = models.ForeignKey(
        Table, verbose_name=Table._meta.verbose_name, related_name='fields')
    name = models.CharField('名称', max_length=64)
    verbose_name = models.CharField('中文名称', max_length=64)
    type = models.CharField('类型', max_length=64)
    foreign_key = models.CharField('外键', max_length=64, null=True, blank=True)
    options = modelutils.JSONField('参数', null=True, blank=True)

    def __unicode__(self):
        return self.name

    def save(self, **kwargs):
        self.source = self.table.source
        if not self.verbose_name:
            self.verbose_name = self.name
        return super(Field, self).save(**kwargs)


class Dictionary(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '字典'

    source = models.ForeignKey(
        Source,
        verbose_name=Source._meta.verbose_name,
        related_name='dictionaries')
    name = models.CharField('名称', max_length=64)
    sql = models.TextField("查询语句")
    directory = models.TextField("目录", null=True, blank=True)
    structure = modelutils.JSONField("结构数据", null=True, blank=True)

    def __unicode__(self):
        return self.name

    def gen_structure(self):
        from django.db import connections
        conn = connections[self.source.name]
        cur = conn.cursor()
        cur.execute(self.sql)
        dr = {}
        for r in cur.fetchall():
            fn, fk, fv = r
            fn = fn.lower()
            dr.setdefault(fn, {'name': fn, 'cases': {}})
            if fk is None or fv is None:
                continue
            dr[fn]['cases'][fk.strip()] = fv.strip()
        return dr

    def save(self, **kwargs):
        self.structure = self.gen_structure()
        self.directory = "\n".join([
                                       "%s\t%s" % (f, fv.get("verbose_name"))
                                       for f, fv in self.structure.iteritems()
                                       ])
        return super(Dictionary, self).save(**kwargs)


class Transfer(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '数据同步'

    name = models.CharField('名称', max_length=64)
    description = models.TextField('简介', null=True, blank=True, default="")
    source = models.ForeignKey(
        Source,
        verbose_name=Source._meta.verbose_name,
        related_name='transfers')
    destination = models.ForeignKey(
        Source, verbose_name="目标库", related_name='destination_transfers')
    dictionaries = models.ManyToManyField(
        Dictionary,
        verbose_name=Dictionary._meta.verbose_name,
        related_name='transfers')
    table_name = models.CharField('表名', max_length=64)
    sql = models.TextField(
        "查询语句", help_text="时间表达式模版为{{begin_time}}和{{end_time}}")
    field_configs = models.TextField(
        "字段设置",
        null=True,
        blank=True,
        help_text="字段名后可加参数, 参数可以指定数据类型,如 varchar:32 index 等")
    field_configs_json = modelutils.JSONField(
        "字段设置Json", null=True, blank=True, default={})

    def __unicode__(self):
        return self.name

    def save(self, **kwargs):
        self.field_configs_json = self.gen_field_configs_json()
        return super(Transfer, self).save(**kwargs)

    def get_dictionaries(self):
        res = {}
        for d in self.dictionaries.all():
            res.update(d.structure)
        return res

    def gen_field_configs_json(self):
        import re
        space = re.compile("\s")
        res = {"index": {}, "type": {}, "dict": {}}
        for l in self.field_configs.split('\n'):
            cs = space.split(l.strip())
            fn = cs[0]
            for c in cs[1:]:
                ps = c.split(":")
                d = res.setdefault(ps[0], {})
                d[fn] = "".join(ps[1:])
        return res

    def run(self, progress=lambda x: None):
        import pandas as pd
        from . import helper
        progress("READ_TABLE")
        df = pd.read_sql(
            self.sql.replace("%", "%%"),
            self.source.alchemy_str)
        dicts = self.get_dictionaries()
        engine = self.source.type
        type_fields = self.field_configs_json.get("type", {})
        dict_fields = self.field_configs_json.get("dict", {})

        progress("DICT_TRANSLATE")
        for fn, dn in dict_fields.iteritems():
            d = dicts[dn or fn]
            nfn = "%s_display_name" % fn
            cases = d.get("cases")
            df[nfn] = df[fn].apply(lambda x: cases.get(unicode(x)))
            type_fields[nfn] = "str64"

        sqlalchemy_types = {}
        for k, v in type_fields.iteritems():
            dtype = helper.get_sqlalchemy_dtype(v, engine)
            if dtype:
                sqlalchemy_types[k] = dtype
        print sqlalchemy_types

        progress("SAVE_DATA")
        df.to_sql(
            self.table_name,
            self.destination.alchemy_str,
            index=False,
            if_exists='replace',
            chunksize=100,
            dtype=sqlalchemy_types)

