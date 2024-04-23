from threading import Thread

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
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            
            if not self.checkRobotTxt(tbd_url):
                self.crawer.log.info(f"Found blackisted site : {tbd_url}")
                continue

            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            
            if(resp.status == 200):
                newLinks = []
                ### Code for questions on assignement
                scraper.updateTokens(self.crawler , resp)
                # Check whether we need robots.txt
                if scraper.checkUniqueNetloc(self.crawler, tbd_url):
                    # get netloc
                    netloc = normalize(urlparse(tbd_url)).netloc
                    # get sitemap URL
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
                    scraped_urls += newLinks
                scraper.updateSubDomains(self.crawler, tbd_url)
                
            ## END
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)


    def checkRobotTxt(self, url):
        # get important strings
        netloc = normalize(urlparse(url).netloc)
        parser = None
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
        except Exception as e:
            return True

        