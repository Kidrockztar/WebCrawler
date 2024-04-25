from threading import Thread, Lock

from inspect import getsource
from utils.download import download
from urllib import robotparser
from utils import get_logger, normalize, get_urlhash
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
            # Check if all of the threads are inactive
            # if they are then stop crawling
            if all(not x for x in self.crawler.activeArray):
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            # set thread status in array
            if not tbd_url:
                self.crawler.activeArray[self.workerId] = False
                continue
            else:
                 self.crawler.activeArray[self.workerId] = True

            # See if it is a new url we are checking
            scraper.updateURLCount(self.crawler, tbd_url)
            # Check if there is a lock already existing in the dict for this hostname
            parsed = urlparse(tbd_url)

            # Lock editing rights to the dict
            with self.crawler.politeLockDictLock:
                # add new lock if hostname is new
                if not parsed.hostname in self.crawler.politeLocksDict:
                    self.crawler.politeLocksDict[parsed.hostname] = Lock()
            
            # Lock the hostname and then do our stuff then wait then free
            with self.crawler.politeLocksDict[parsed.hostname]:
                
                if not self.checkRobotTxt(tbd_url):
                    self.crawler.logger.info(f"Found blackisted site : {tbd_url}")
                    continue

                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                # Wait for keeping lock locked while not polite
                time.sleep(self.config.time_delay)
            
            scraped_urls = scraper.scraper(self.crawler, tbd_url, resp)
            if(resp.status == 200):
                
                ### Code for questions on assignement
                scraper.updateTokens(self.crawler , resp)
                scraper.updateSubDomains(self.crawler, tbd_url)
                # Check whether we need robots.txt
                if scraper.checkUniqueNetloc(self.crawler, tbd_url):
                    self.crawler.logger.info("Getting robot txt")
                    # get netloc
                    netloc = normalize(parsed.netloc)
                    # get robots.txt URL
                    robots_url = parsed.scheme + "://" + netloc + "/robots.txt"
                    self.crawler.logger.info("Parsing robots.txt")
                    resp = download(robots_url, self.frontier.config)
                    
                    # Check if the response is successful
                    if resp.status == 200:
                        # Parse robots.txt content
                        robots_content = resp.raw_response.content.decode('utf-8')
                        sitemap_urls = []
                        for line in robots_content.split('\n'):
                            if line.startswith("Sitemap:"):
                                sitemap_urls.append(line.split(":", 1)[1].strip())

                        # Function to recursively parse sitemaps
                        def parse_sitemap(sitemap_url):
                            sitemap_resp = download(sitemap_url, self.frontier.config)
                            sitemap_soup = BeautifulSoup(sitemap_resp.raw_response.content, "xml")
                            sitemap_links = sitemap_soup.find_all("url")
                            for link in sitemap_links:
                                if link:
                                    # Get the location within the url tags
                                    actualURL = link.find("loc").text
                                    print(f"appending {actualURL}")
                                    scraped_urls.append(actualURL)

                            nested_sitemaps = sitemap_soup.find_all("sitemap")
                            for nested_sitemap in nested_sitemaps:
                                nested_sitemap_url = nested_sitemap.find("loc").text
                                parse_sitemap(nested_sitemap_url)

                        # Parse each sitemap recursively
                        self.crawler.logger.info("Parsing sitemaps")
                        for sitemap_url in sitemap_urls:
                            parse_sitemap(sitemap_url)
                                
                ## END
                # Remove none types
                scraped_urls = [link for link in scraped_urls if link]

                # add to be searched
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                self.frontier.mark_url_complete(tbd_url)


    def checkRobotTxt(self, url):
        # get important strings
        parsed = urlparse(url)
        parsedNetloc = parsed.netloc
        parsedScheme = parsed.scheme

        # Sometimes netloc can be in bytes, ifso, decode to string
        if isinstance(parsedNetloc, bytes):
            parsedNetloc = netloc.decode('utf-8')  
        if isinstance(parsedScheme, bytes):
            parsedScheme = parsedScheme.decode('utf-8')  
        
        netloc = normalize(parsedNetloc.strip("www."))

        with self.crawler.robotTxtLock:
            try:
                if netloc in self.crawler.robotTxt.keys():
                    parser = self.crawler.robotTxt[netloc]
                    return parser.can_fetch(self.crawler.config.user_agent, url)
                else:
                    parser = robotparser.RobotFileParser()
                    parser.set_url(parsedScheme + "://" + netloc + "/robots.txt")
                    parser.read()
                    self.crawler.robotTxt[netloc] = parser
                    return parser.can_fetch(self.crawler.config.user_agent, url)
            # if something wennt wrong we can assume that the robot txt says we are good
            except Exception as e:
                self.crawler.logger.info(f"{e} on {netloc}")
                return True

        