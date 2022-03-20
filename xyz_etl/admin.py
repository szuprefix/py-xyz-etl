# -*- coding:utf-8 -*-
from django.contrib import admin

# Register your models here.

from . import models, helper


def init_source(modeladmin, request, queryset):
    helper.init_source()


init_source.short_description = u"登记数据源"


def init_table(modeladmin, request, queryset):
    for obj in queryset.all():
        helper.init_table(obj)


init_table.short_description = u"遍历数据表"


class SourceAdmin(admin.ModelAdmin):
    list_display = ('name', 'verbose_name', 'type', 'host')
    actions = [init_table, init_source]


admin.site.register(models.Source, SourceAdmin)


class TableAdmin(admin.ModelAdmin):
    list_display = ('name', 'verbose_name', 'source')
    list_filter = ('source',)
    search_fields = ('name',)
    raw_id_fields = ('source',)


admin.site.register(models.Table, TableAdmin)


class FieldAdmin(admin.ModelAdmin):
    list_display = ('name', 'table', 'source')
    raw_id_fields = ('source', 'table')
    list_filter = ('source',)
    search_fields = ('table__name',)


admin.site.register(models.Field, FieldAdmin)


class DictionaryAdmin(admin.ModelAdmin):
    list_display = ('name', 'source')
    raw_id_fields = ('source',)
    readonly_fields = ('structure', )


admin.site.register(models.Dictionary, DictionaryAdmin)


class TransferAdmin(admin.ModelAdmin):
    list_display = ('name', 'source', 'destination')
    readonly_fields = ('field_configs_json',)
    raw_id_fields = ("source", "destination", "dictionaries")

admin.site.register(models.Transfer, TransferAdmin)
