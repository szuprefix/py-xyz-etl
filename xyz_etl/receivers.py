# -*- coding:utf-8 -*- 
__author__ = 'denishuang'
from django.dispatch import receiver
from django.db.models.signals import post_save
from . import models


# @receiver(post_save, sender=models.Corporation)
# def initCorporationDepartment(sender, **kwargs):
#     corporation = kwargs['instance']
#     if corporation.departments.count() == 0:
#         corporation.departments.create(name=u'销售部')

