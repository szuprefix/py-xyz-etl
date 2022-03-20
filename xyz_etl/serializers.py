# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

from rest_framework import serializers
from . import models
from xyz_restful.mixins import IDAndStrFieldSerializerMixin


class SourceSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    class Meta:
        model = models.Source
        exclude = ()


class TableSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    source_name = serializers.CharField(source="source.__str__", read_only=True)
    class Meta:
        model = models.Table
        exclude = ()

class TableListSerializer(TableSerializer):
    class Meta(TableSerializer.Meta):
        exclude = ('meta', )


class DictionarySerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    source_name = serializers.CharField(source="source.__str__", read_only=True)
    class Meta:
        model = models.Dictionary
        exclude = ()


class TransferSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    source_name = serializers.CharField(source="source.__str__", read_only=True)
    destination_name = serializers.CharField(source="destination.__str__", read_only=True)

    class Meta:
        model = models.Transfer
        # fields = ('name', 'table_name', 'source', 'source_name', 'destination', 'destination_name', 'sql')
        exclude = ()
        read_only_fields = ('field_configs_json', )
