from threading import Thread, Lock

from inspect import getsource
from utils.download import download
from urllib import robotparser
from utils import get_logger, normalize
import scraper
import time
from urllib.parse import urlparse
from bs4 import BeautifulSoup


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, crawler):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.crawler = crawler
        self.workerId = worker_id
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            #
            if all(not x for x in self.crawler.activeArray):
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            if not tbd_url:
                self.crawler.activeArray[self.workerId] = False
                continue
            else:
                 self.crawler.activeArray[self.workerId] = True

            # Check whether we are still being polite
            #self.politeLockDictLock = threading.lock()
            #self.politeLocksDict = {}

            # Check if there is a lock already existing in the dict for this hostname
            parsed = urlparse(tbd_url)
            # Lock editing rights to the dict
            with self.crawler.politeLockDictLock:
                if not parsed.hostname in self.crawler.politeLocksDict:
                    self.crawler.politeLocksDict[parsed.hostname] = Lock()
            
            # Lock the hostname and then do our stuff then wait then free
            with self.crawler.politeLocksDict[parsed.hostname]:
                
                if not self.checkRobotTxt(tbd_url):
                    self.crawer.log.info(f"Found blackisted site : {tbd_url}")
                    continue

                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                # Wait for keeping lock locked while not polite
                time.sleep(self.config.time_delay)
            
            scraped_urls = scraper.scraper(tbd_url, resp)
            if(resp.status == 200):
                
                newLinks = []
                ### Code for questions on assignement
                scraper.updateTokens(self.crawler , resp)
                scraper.updateSubDomains(self.crawler, tbd_url)
                scraper.updateURLCount(self.crawler, tbd_url)
                
                # Check whether we need robots.txt
                if scraper.checkUniqueNetloc(self.crawler, tbd_url):
                    # get netloc
                    netloc = normalize(urlparse(tbd_url)).netloc
                    # get sitemap URL
                    # reserve the robot txt lock in case somebody tries to add the parser after
                    if netloc in self.crawler.robotTxt:
                        siteMapURL = self.crawler.robotTxt[netloc].site_map()
                        if siteMapURL:
                            resp = download(netloc+siteMapURL)
                            soup = BeautifulSoup(resp.raw_response.content, "lxml")
                            sitemap_tags = soup.find_all("sitemap")
                            newLinks += soup.find_all("url")

                            # loop over any found sitemaps and also add their links
                            for sitemap in sitemap_tags:
                                resp = download(netloc+sitemap)
                                soup = BeautifulSoup(resp.raw_response.content, "lxml")
                                newLinks += soup.find_all("url")
                    
                    # add new site map links in
                    if newLinks:
                        scraped_urls += [link for link in newLinks if scraper.is_valid(link)]
                
            ## END
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)

                


    def checkRobotTxt(self, url):
        # get important strings
        netloc = normalize(urlparse(url).netloc)
        parser = None

        with self.crawler.robotTxtLock:
            try:
                if netloc in self.crawler.robotTxt.keys():
                    parser = self.crawler.robotTxt[netloc]
                    return parser.can_fetch(self.crawler.config.user_agent, url)
                else:
                    parser = robotparser.RobotFileParser()
                    parser.set_url(netloc + "/robots.txt")
                    parser.read()
                    self.crawler.robotTxt[netloc] = parser
                    return parser.can_fetch(self.crawler.config.user_agent, url)
            # if something wennt wrong we can assume that the robot txt says we are good
            except Exception as e:
                return True

        