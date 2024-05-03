from threading import Thread, Lock

from inspect import getsource
from utils.download import download
from urllib import robotparser
from utils import get_logger, normalize, get_urlhash
import scraper
import time
from urllib.parse import urlparse, urljoin
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
            # if they are then stop crawling on all threads
            if all(not x for x in self.crawler.activeArray):
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            # set thread status in array
            if not tbd_url:
                self.crawler.activeArray[self.workerId] = False
                continue
            else:
                 self.crawler.activeArray[self.workerId] = True
                 
            # Check if there is a lock already existing in the dict for this hostname
            parsed = urlparse(tbd_url)

            # Lock editing rights to the dict
            with self.crawler.politeLockDictLock:
                # add new lock if hostname is new
                if not parsed.hostname in self.crawler.politeLocksDict:
                    self.crawler.politeLocksDict[parsed.hostname] = Lock()
            
            # Lock the hostname and then do our stuff then wait then free
            with self.crawler.politeLocksDict[parsed.hostname]:
                
                # Check if we have permission to crawl the site
                if not self.checkRobotTxt(tbd_url):
                    self.crawler.logger.info(f"Found blackisted site : {tbd_url}")
                    self.frontier.remove_url(tbd_url)
                    continue

                resp = download(tbd_url, self.config, self.logger)

                # Wait for keeping lock locked while not polite
                time.sleep(self.config.time_delay)

                # Handle none type
                if resp and resp.raw_response:
                    self.logger.info(
                        f"Downloaded {tbd_url}, status <{resp.status}>, "
                        f"using cache {self.config.cache_server}.")

                    # Handle website redirects and make sure we index the redirected content
                    if (resp.status == 301 or resp.status == 302 or tbd_url != resp.url):
                        # Delete url from frontier 
                        self.frontier.remove_url(tbd_url)
                        # Add redirected url
                        self.frontier.add_url(resp.url)
                    
                    # Update the count of the URLS
                    scraper.updateURLCount(self.crawler, resp.url)
                    
                    if(resp.status == 200 or resp.status == 301 or resp.status == 302):
                        
                        # Actually scrape the response
                        scraped_urls = scraper.scraper(self.crawler, resp.url, resp)
                        ### Code for questions on assignement
                        scraper.updateTokens(self.crawler , resp)
                        scraper.updateSubDomains(self.crawler, resp.url)
                        # Check whether we need robots.txt
                        if scraper.checkUniqueNetloc(self.crawler, resp.url):
                            self.crawler.logger.info("Getting robot txt")
                            # get netloc
                            netloc = normalize(parsed.netloc)
                            # get robots.txt URL
                            robots_url = parsed.scheme + "://" + netloc + "/robots.txt"
                            self.crawler.logger.info("Parsing robots.txt")
                            robotResp = download(robots_url, self.frontier.config)

                            # Maintain politness with robot txt request
                            time.sleep(self.config.time_delay)

                            # Check again that the response is not a none type
                            if robotResp:
                                # Check if the response is successful
                                if robotResp.status == 200:
                                    # Parse robots.txt content
                                    robots_content = robotResp.raw_response.content.decode('utf-8')
                                    sitemap_urls = []
                                    for line in robots_content.split('\n'):
                                        if line.startswith("Sitemap:"):
                                            sitemap_urls.append(line.split(":", 1)[1].strip())

                                    # Function to recursively parse sitemaps
                                    def parse_sitemap(sitemap_url):
                                        # Download the xml file from the website
                                        sitemap_resp = download(sitemap_url, self.frontier.config)

                                        # Maintain politness with robot txt request
                                        time.sleep(self.config.time_delay)
                                        
                                        # Handle the none object return case
                                        if sitemap_resp and sitemap_resp.raw_response:
                                            # Use beautiful soup to parse the xml
                                            sitemap_soup = BeautifulSoup(sitemap_resp.raw_response.content, "xml")

                                            # Get all instances of url in the site map and extract the link
                                            #<url> <loc> </loc> </url>
                                            sitemap_links = sitemap_soup.find_all("url")
                                            for link in sitemap_links:
                                                if link:
                                                    # Get the location within the url tags
                                                    actualURL = link.find("loc").text
                                                    print(f"appending {actualURL}")
                                                    if scraper.is_valid(self.crawler, actualURL):
                                                        scraped_urls.append(urljoin(resp.url, actualURL))

                                            # If there are nested sitemaps call recursive
                                            nested_sitemaps = sitemap_soup.find_all("sitemap")
                                            for nested_sitemap in nested_sitemaps: 
                                                nested_sitemap_url = nested_sitemap.find("loc").text
                                                parse_sitemap(nested_sitemap_url)
                                        else:
                                            # Return nothing if the site map wasn't succesfully gotten
                                            return []

                                    # Parse each sitemap recursively
                                    self.crawler.logger.info("Parsing sitemaps")
                                    for sitemap_url in sitemap_urls:
                                        parse_sitemap(sitemap_url)
                                        
                        ## END
                        # Another level of insurance to catch none type links
                        scraped_urls = [link for link in scraped_urls if link]

                        # add found links to be searched in the frontier
                        for scraped_url in scraped_urls:
                            if scraper.is_valid(self.crawler, scraped_url):
                                self.frontier.add_url(scraped_url)
                
                # Mark this url complete regardless of the outcome
                self.frontier.mark_url_complete(resp.url)
                


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
        
        # sometimes the netloc has www which crashes the code
        netloc = normalize(parsedNetloc.strip("www."))

        # Reserve the shelve
        with self.crawler.robotTxtLock:
                # Check if the robot txt has already been fetched
                if netloc in self.crawler.robotTxt.keys():
                    parser = self.crawler.robotTxt[netloc]
                    return parser.can_fetch(self.crawler.config.user_agent, url)
                else:
                    # fetch new robot txt
                    parser = robotparser.RobotFileParser()
                    parser.set_url(parsedScheme + "://" + netloc + "/robots.txt")
                    try:
                        parser.read()
                        # Maintain politness with robot txt request
                        time.sleep(self.config.time_delay)
                        self.crawler.robotTxt[netloc] = parser
                        return parser.can_fetch(self.crawler.config.user_agent, url)
                    except Exception as e:
                        return True

        