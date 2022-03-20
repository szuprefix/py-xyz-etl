# -*- coding:utf-8 -*-
__author__ = 'denishuang'
from . import serializers, models, helper, tasks
from rest_framework import viewsets, status, decorators
from rest_framework.response import Response
from xyz_restful.decorators import register


@register()
class SourceViewSet(viewsets.ModelViewSet):
    queryset = models.Source.objects.all()
    serializer_class = serializers.SourceSerializer
    search_fields = ('name', 'verbose_name', 'host')
    filter_fields = ('type',)

    @decorators.action(['POST'], detail=False)
    def discover(self, request):
        helper.init_source()
        return Response({})

    @decorators.action(['POST', 'GET'], detail=True)
    def discover_table(self, request, pk=None):
        tns = helper.init_table(self.get_object())
        return Response({'table_names': tns})

    @decorators.action(['GET'], detail=True)
    def query(self, request, pk=None):
        source = self.get_object()
        sql = request.query_params.get("sql")
        from xyz_util import pandasutils
        try:
            df = source.query(sql)
            data = pandasutils.dataframe_to_table(df)
        except Exception, e:
            data = {'error': unicode(e)}
        return Response(data)

    @decorators.action(['GET'], detail=True)
    def detect_schemas(self, request, pk=None):
        source = self.get_object()
        source.schemas = source.detect_schemas()
        source.save()
        return Response(serializers.SourceSerializer(source).data)


@register()
class TableViewSet(viewsets.ModelViewSet):
    queryset = models.Table.objects.all()
    serializer_class = serializers.TableSerializer
    filter_fields = ("source", "is_active")
    search_fields = ("name", "verbose_name")

    def get_serializer_class(self):
        if self.action == 'list':
            return serializers.TableListSerializer
        return super(TableViewSet, self).get_serializer_class()

    @decorators.action(['GET'], detail=True)
    def count(self, request, pk=None):
        table = self.get_object()
        return Response({"count": table.count()})

    @decorators.action(['POST'], detail=True)
    def detect_meta(self, request, pk=None):
        table = self.get_object()
        table.meta = table.detect_meta()
        table.save()
        return Response(self.serializer_class(table).data)


@register()
class DictionaryViewSet(viewsets.ModelViewSet):
    queryset = models.Dictionary.objects.all()
    serializer_class = serializers.DictionarySerializer
    search_fields = ("name",)
    # read_only_fields = ('fields_phrase',)


@register()
class TransferViewSet(viewsets.ModelViewSet):
    queryset = models.Transfer.objects.all()
    serializer_class = serializers.TransferSerializer
    search_fields = ("name", "table_name")

    @decorators.action(['POST', 'GET'], detail=True)
    def run(self, request, pk=None):
        transfer = self.get_object()
        rs = tasks.run_transfer.delay(transfer.id)
        return Response(dict(task=dict(id=rs.id, status=rs.status)), status=status.HTTP_201_CREATED)
