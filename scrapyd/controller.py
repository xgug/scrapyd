import sys
import uuid
import os
from datetime import datetime
from multiprocessing import cpu_count

from twisted.internet import reactor, defer, protocol, error
from twisted.application.service import Service
from twisted.python import log
from twisted.application.service import IServiceCollection

from scrapyd.utils import get_crawl_args, native_stringify_dict
from scrapyd import __version__
from .interfaces import IPoller, IEnvironment

from collections import defaultdict
from .spiderqueue import  RedisSpiderQueue
from .interfaces import IEggStorage, IPoller, ISpiderScheduler, IEnvironment


class Controller(Service):
    name = 'controller'

    def __init__(self, config, app):
        # self.processes = {}
        # self.finished = []
        # self.finished_to_keep = config.getint('finished_to_keep', 100)
        # self.max_proc = self._get_max_proc(config)
        # self.runner = config.get('runner', 'scrapyd.runner')
        self.app = app
        self.config = config
        self.spider_config = dict(config.items('spdier'))

    def startService(self):
        # for slot in range(self.max_proc):
        #   print('=='*100)
        #   print(slot)
        #
        # print('-'*100)
        # print(self.max_proc)
        print(self.config._getsources())
        print('-=' * 50)
        # launcher = self.app.getServiceNamed('launcher')
        print(self.launcher.processes.values())

    def poll_test(self):
        print('-' * 50 + 'poll test!' + '-' * 50)

    @property
    def launcher(self):
        app = IServiceCollection(self.app, self.app)
        return app.getServiceNamed('launcher')

    @property
    def scheduler(self):
        return self.app.getComponent(ISpiderScheduler)

    def _get_available_spiders(self, spider):
        config = dict(self.config.items('spider_monitor_config'))
        path = config['stop_tag_path']

        spider_stop_file = ''.join([path, spider, '.stop'])
        stop_all_tag_file = ''.join([path, 'stop_all_spider.stop'])

        return spider in self.spider_config and not os.path.exists(spider_stop_file) and not os.path.exists(stop_all_tag_file)

    def _get_running_spider_count(self, project):
        spiders = self.launcher.processes.values()

        running = defaultdict(int)
        # log.msg('project and process', project, spiders)

        for s in spiders:

            if s.project == project and self._get_available_spiders(s.spider):
                running[s.spider] += 1

        return running

    def _get_spider_data_lenght(self, spider_name):
        """get count of spider data from redis queue"""
        c = dict(self.config.items('redis'))
        password = c['password'] if c['password'] else None

        return RedisSpiderQueue(spider_name, db=int(c['db']), password=password, host=c['host'], port=int(c['port'])).count()

    def _get_spider_conf_process_total(self, spider_name):
        """config of running spdier"""

        split_piece = int(self.config.get('split_piece', 2000))

        update_data_total = self._get_spider_data_lenght(spider_name)
        need_spiders = int(update_data_total / split_piece)

        limit_total = int(self.spider_config.get(spider_name, 0))
        if need_spiders > limit_total:
            need_spiders = limit_total

        return need_spiders

    def _add_spider_process(self, project, spider, total):
        """for schedule total of total spdier."""
        jobid = uuid.uuid1().hex
        args = {'_job': jobid}

        for x in range(total):
            self.scheduler.schedule(project, spider, **args)
            log.msg('start <spdier> : %s', spider)

    def _if_cancel_spider_process(self, project, spider_name, total):
        """ stop spdier"""
        total = -total if total < 0 else total

        running_spiders = [s for s in self.launcher.processes.values() if s.spider==spider_name and s.project==project]

        spiders = defaultdict(int)
        log.msg('project and process', project, running_spiders)

        for s in running_spiders:

            if self._get_available_spiders(s.spider):
                spiders[s.spider] += 1

        # spiders = self._get_running_spider_count(project)
        stop_jobs = []
        if total > 0 and spiders.get(spider_name, 0) > total:

            stop_spiders = running_spiders[0:total]
            for s in stop_spiders:
                s.transport.signalProcess('TERM')
                stop_jobs.append({'spider': s.spider, 'jobid': s.job})

        log.msg('stop spiders : %s', stop_jobs)

        return stop_jobs

    def poll_monitor_spider_process(self):
        project = self.config.get('project')
        log.msg('poll monitor process of spdier. <project> ', project)
        running_spiders = self._get_running_spider_count(project)

        for spider_name, total in running_spiders.items():
            # checkout the spider is exist in spider config.
            if spider_name in self.spider_config:
                config_total = self._get_spider_conf_process_total(spider_name)
                n_t = config_total - total

                log.msg('Need to handle <spider> <total> ', spider_name, n_t)

                if n_t > 0:
                    # added new process of spider for needing total of config.
                    self._add_spider_process(project, spider_name, n_t)
                elif n_t < 0:
                    self._if_cancel_spider_process(project, spider_name, n_t)
