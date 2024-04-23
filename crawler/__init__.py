from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
import os
import shelve
import threading



class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

        # stuff for storing longest file
        self.longestFile = shelve.open("longest.shelve")
        self.longest = 0
        self.longestLock = threading.Lock()

        # Unique pages storage
        self.uniquePages = shelve.open("uniquePages.shelve") 
        self.uniquePagesLock = threading.Lock()

        # Keep track of all tokens on the site
        self.tokens = shelve.open("tokens.shelve")
        self.tokensLock = threading.Lock()

        # Keep track of current subdomains list
        self.icsSubDomainCounts = shelve.open("subDomains.shelve")
        self.icsSubDomainCountsLock = threading.Lock()

        # Used to identify whether we should find the sitemap
        self.netlocs = shelve.open("netloc.shelve")
        self.netlocsLock = threading.Lock()

        # place to store the robot .txt parsers
        self.robotTxt = shelve.open("robotTXTs.txt")
        self.robotTxtLock = threading.Lock()

        # Place to maintain politness between threads
        self.politeLockDictLock = threading.Lock()
        self.politeLocksDict = {}

        # threads being done array
        self.activeArray = []
        for id in range(self.config.threads_count):
            self.activeArray.append(True)

        if restart:
            print("clearing all shelves")
            self.longestFile.clear()
            self.longest = 0
            self.uniquePages.clear()
            self.tokens.clear()
            self.icsSubDomainCounts.clear()
            self.netlocs.clear()

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
    