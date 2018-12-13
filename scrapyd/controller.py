import uuid
import os

from twisted.application.service import Service
from twisted.python import log
from twisted.application.service import IServiceCollection

from collections import defaultdict
from .spiderqueue import RedisSpiderQueue
from .interfaces import ISpiderScheduler, IPoller


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
        self.section_suffix = os.environ.get('REGION', 'us')

        self.spider_section = ''.join(['spdier', '_', self.section_suffix])
        self.redis_section = ''.join(['redis', '_', self.section_suffix])
        self.project = self.config.get('project')

    def startService(self):

        log.msg('The monitor of spider process is start!')

    def stopService(self):
        log.msg('The monitor of spider process is stop!!!')

        stop_jobs = []
        for s in self.launcher.processes.values():
            s.transport.signalProcess('TERM')
            stop_jobs.append({'spider': s.spider, 'jobid': s.job})

        log.msg('Stop the process:', stop_jobs)


    @property
    def launcher(self):
        app = IServiceCollection(self.app, self.app)
        return app.getServiceNamed('launcher')

    @property
    def scheduler(self):
        return self.app.getComponent(ISpiderScheduler)

    @property
    def poller(self):
        return self.app.getComponent(IPoller)

    def _get_available_spiders(self, spider):
        config = dict(self.config.items('spider_monitor_config'))
        path = config['stop_tag_path']

        spider_stop_file = ''.join([path, spider, '.stop'])
        stop_all_tag_file = ''.join([path, 'stop_all_spider.stop'])

        return spider in self.spider_config and not os.path.exists(spider_stop_file) and not os.path.exists(stop_all_tag_file)

    @property
    def _all_spider_count(self):
        project = self.project
        processes = self.launcher.processes.values()
        queues = self.poller.queues
        # log.msg("processes: %s", processes)

        running = [{"project": project, "spider": p.spider, "id": p.job} for p in processes
                   if p.project == project]

        pending = [{"project": project, "spider": x["name"], "id": x["_job"]} for x in queues[project].list()]

        spiders = running + pending

        total_spiders = defaultdict(int)
        # log.msg('project and process', project, spiders)

        for s in spiders:
            if self._get_available_spiders(s.get('spider', 'none')):
                total_spiders[s.get('spider', 'none')] += 1

        return total_spiders

    def _get_spider_count(self, spider_name):

        return self._all_spider_count.get(spider_name, 0)

    def _get_spider_data_lenght(self, spider_name):
        """get count of spider data from redis queue"""
        c = self.redis_config
        password = c.get('password', None)

        return RedisSpiderQueue(key=spider_name, db=int(c['db']), password=password, host=c['host'], port=int(c['port'])).count()

    def _get_spider_conf_process_total(self, spider_name):
        """config of running spdier"""

        try:
            update_data_total = self._get_spider_data_lenght(spider_name)
        except Exception as e:
            update_data_total = 0
            log.msg('redis connect fail!!! : ', e)

        # need_spiders = int(math.ceil(Decimal(update_data_total) / Decimal(split_piece)))

        limit_total = int(self.spider_config.get(spider_name, 0))

        need_spiders = 0
        if update_data_total > 0:
            need_spiders = limit_total - self._get_spider_count(spider_name)

            # if need_spiders < 0:
            #     need_spiders = 0

        return need_spiders

    def _add_spider_process(self, project, spider_name, total):
        """for schedule total of total spdier."""
        jobid = uuid.uuid1().hex
        args = {'_job': jobid}

        if self._get_available_spiders(spider_name):
            for x in range(total):
                self.scheduler.schedule(project, spider_name, **args)
                log.msg('start <spdier> : ', spider_name)

    def _if_cancel_spider_process(self, project, spider_name, total):
        """ stop spdier"""
        total = -total if total < 0 else total

        running_spiders = [s for s in self.launcher.processes.values() if s.spider == spider_name and s.project == project]

        spiders = defaultdict(int)
        log.msg('project and process', project, running_spiders)

        for s in running_spiders:
            # if self._get_available_spiders(s.spider):
            spiders[s.spider] += 1

        stop_jobs = []
        if spiders.get(spider_name, 0) > total:

            stop_spiders = running_spiders[0:total]
            for s in stop_spiders:
                # s.transport.signalProcess('TERM')
                s.transport.signalProcess('KILL')
                stop_jobs.append({'spider': s.spider, 'jobid': s.job})

        log.msg('stop spiders : ', stop_jobs)

        return stop_jobs

    def _check_redis(self):
        """check redis is available."""
        c = self.redis_config
        password = c.get('password', None)

        try:
            RedisSpiderQueue(key='test', db=int(c['db']), password=password, host=c['host'], port=int(c['port'])).count()
            return True
        except Exception as e:
            return False

    def poll_monitor_spider_process(self):
        project = self.project
        log.msg('poll monitor process of spdier. <project> ', project)
        if not self.config.has_section(self.spider_section) and not self.config.has_section(self.redis_section):
            log.msg("Have not config of spider_section or redis_section at region: ", os.environ.get('REGION', 'us'))

            return

        self.spider_config = dict(self.config.items(self.spider_section))
        self.redis_config = dict(self.config.items(self.redis_section))

        if not self._check_redis():
            log.msg("Redis is unavailable, please check it!")

            return

        # for spider_name, total in running_spiders.items():
        for spider_name in self.spider_config.keys():
            # checkout the spider is exist in spider config.
            config_total = self._get_spider_conf_process_total(spider_name)
            # n_t = config_total - int(running_spiders.get(spider_name, 0))

            if config_total > 0:
                log.msg('Need to handle add', ' <total> ', config_total, ' <spider> ', spider_name)
                # added new process of spider for needing total of config.
                self._add_spider_process(project, spider_name, config_total)
            elif config_total < 0:
                log.msg('Need to handle cancel', ' <total> ', config_total, ' <spider> ', spider_name)

                self._if_cancel_spider_process(project, spider_name, config_total)
