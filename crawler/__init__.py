from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
import os
import shelve



class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory
        self.longestFile = shelve.open("longest.shelve")
        self.longest = 0
        self.uniquePages = shelve.open("uniquePages.shelve") 
        self.tokens = shelve.open("tokens.shelve")
        self.icsSubDomainCounts = shelve.open("subDomains.shelve")

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
    