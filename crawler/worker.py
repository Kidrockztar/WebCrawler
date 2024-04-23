from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger, normalize
import scraper
import time
from urllib.parse import urlparse


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
                continue

            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            
            if(resp.status == 200):
                ### Code for questions on assignement
                scraper.updateTokens(self.crawler , resp)
                scraper.updateURLCount(self.crawler, tbd_url)
                scraper.updateSubDomains(self.crawler, tbd_url)
                
            ## END
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)


    def checkRobotTxt(self, url):
        # get important strings
        parsed = urlparse(url)
        normalPath = normalize(parsed.path)
        normalNetloc = normalize(parsed.netloc)

        # Check if there is an entry for txt
        if normalNetloc in self.crawler.robotTxt:
            # iterate over the paths in the list and see if it applies
            for path in self.crawler.robotTxt[normalNetloc]:
                if normalPath.startswith(path):
                    return False
            else:
                return True

        else:
            # Request robots txt file
            robotURL = parsed.scheme + parsed.netloc + "/robots.txt"
            print(robotURL)
            resp = download(robotURL, self.config, self.logger)
            if resp:
                lines = resp.raw_response.content
                # Set the empty list for appending
                self.crawler.robotTxt[normalNetloc] = []
                
                # get names
                agent_name = self.crawler.config.user_agent
                self.crawler.robotTxt
                applying_flag = False
                # parse lines
                for line in lines.split('\n'):
                    if line.strip():
                        # Split stuff up 
                        key, value = line.split(':', 1)
                        key = key.strip().lower()
                        # remove spaces
                        value = value.strip()
                        # remove wildcards
                        value = value.strip("*")
                        if key == "user-agent" and (agent_name in value or value == "*"):
                            applying_flag = True
                        if applying_flag:
                            if key == 'disallow':
                                self.crawler.robotTxt[normalNetloc].append(normalize(value)) 
                
                print(f"recursing with :{url}, {self.crawler.robotTxt[normalNetloc]}")
                return self.checkRobotTxt(url)
            else:
                print("invalid response")