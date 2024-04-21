from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


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

            ### 1. It is important to filter out urls that do not point to a webpage. For
            ### example, PDFs, PPTs, css, js, etc. The is_valid filters a large number of
            ### such extensions, but there may be more.
            ### 2. It is important to filter out urls that are not with ics.uci.edu domain.
            ### 3. It is important to maintain the politeness to the cache server (on a per
            ### domain basis).
            ### 4. It is important to set the user agent in the config.ini correctly to get
            ### credit for hitting the cache servers.
            ### 5. Launching multiple instances of the crawler will download the same urls in
            ### both. Mechanisms can be used to avoid that, however the politeness limits
            ### still apply and will be checked.
            ### 6. Do not attempt to download the links directly from ics servers.
            
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            
            if(resp.status == 200):
                ### Code for questions on assignement
                scraper.updateTokens(self.crawler , resp)
                #scraper.updateURLCount(self.crawler, tbd_url) // just check length of db
                scraper.updateSubDomains(self.crawler, tbd_url)
            
            ## END
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
