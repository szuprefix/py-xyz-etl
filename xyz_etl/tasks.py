# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
from celery import shared_task
from . import helper, models

from celery.utils.log import get_task_logger

log = get_task_logger(__name__)

@shared_task(bind=True, time_limit=1800)
def run_transfer(self, id):
    ts = models.Transfer.objects.get(id=id)
    ts.run(progress=lambda s: self.update_state(state=s))
    return "success"

