from __future__ import unicode_literals

from django.apps import AppConfig


class Config(AppConfig):
    name = 'etl'

    def ready(self):
        from .helper import init_source
        # init_source()
