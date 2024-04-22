import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import crawler
import shelve
from utils import get_logger, get_urlhash, normalize


stopWords = {"we'd", 'his', "you're", 'its', "mustn't", "i'd", "you've", 'that', 'nor', 'only', 'both', 'because', 'through', 'from', 'herself', 'same', 'themselves', 'having', 'this', "we're", 'further', 'your', 'which', "that's", 'down', 'been', 'more', "weren't", 'why', 'with', 'some', 'them', 'below', 'their', "couldn't", 'if', 'then', 'in', 'about', 'i', 'of', "wouldn't", "she's", 'all', "i'm", 'than', 'what', 'when', 'against', 'so', 'he', 'did', "hadn't", 'those', "aren't", 'here', 'yours', "it's", 'be', 'until', "when's", 'no', 'an', "don't", 'not', 'were', "doesn't", 'me', 'on', "there's", 'at', 'any', 'out', "i've", 'over', 'have', 'has', 'we', "they've", "wasn't", "we'll", 'yourselves', 'whom', "hasn't", "they'll", 'a', 'to', 'but', "he'd", 'am', 'her', 'above', 'under', 'the', 'after', "they'd", 'doing', "haven't", 'should', 'him', 'is', 'other', "shouldn't", 'how', 'cannot', 'they', "i'll", 'itself', 'myself', 'himself', 'between', 'it', 'would', 'my', "they're", "she'll", 'ours', 'or', 'was', 'where', "won't", "can't", 'too', "here's", "where's", 'again', 'into', 'most', "let's", 'does', 'by', 'being', 'these', 'such', "he'll", "isn't", "didn't", "who's", 'few', "you'd", 'you', 'do', 'each', 'ourselves', "we've", 'yourself', 'who', 'during', 'our', 'are', "what's", "you'll", 'and', 'as', 'hers', 'once', 'up', 'off', "shan't", 'she', 'there', 'while', "he's", 'could', "how's", 'very', 'before', 'ought', 'for', 'had', "she'd", "why's", 'own', 'theirs'}   


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    linkList = []
    if resp.status == 200:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        # Get all of the link tags and add them to the list
        for link in soup.find_all('a'):
            linkList.append(link.get('href'))
    else:
        print(f"Failed to retrieve the web page. Status code: {resp.status}")

    return linkList

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
       
        parsed = urlparse(url) 
         # returns <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
        if not checkDomain(parsed.netloc):
            return False
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def checkDomain(netloc):
    domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
    parsed = netloc
    # can sometimes throw back bytes depending on the website
    if isinstance(netloc, bytes):
       parsed = netloc.decode('utf-8')  
    for d in domains:
        if d in parsed:
            return True
    return False

def updateTokens(crawler : crawler, resp):
    soup = BeautifulSoup(resp.raw_response.content, 'html.parser')

    # get each paragraph and read all of them
    text = ""
    for paragraph in soup.find_all("p"):
        text += paragraph.get_text() + " "
    
    # tokenize it 
    text = tokenize_text(text)

    # remove stopwords and punctuation
    tokens = [t for t in text if t not in stopWords]
    
    if(len(tokens) > crawler.longest):
        crawler.longestFile.clear() 
        crawler.longestFile[normalize(resp.url)] = len(tokens)
        crawler.longest = len(tokens)
        

    # build frequency dict
    tokens = computeWordFrequencies(tokens)

    # update counts
    for k,v in tokens.items():
        if k in crawler.tokens:
            crawler.tokens[k] += v
        else:
            crawler.tokens[k] = v

def tokenize_text(text):

    tokens = []
    current_token = []
    for char in text:
        if char.isalnum() or char == "'":
            current_token.append(char)
        else:
            if len(current_token) >= 1:
                tokens.append(''.join(current_token).lower())
                current_token = []
        
    return tokens


# Computing the word frequencies is also O(N) where N is the ammount of words in the list
def computeWordFrequencies(TokenList : list) -> dict:
    # returns map of tokens to counts
    wordFrequencies = dict()
    for t in TokenList:
        if t in wordFrequencies.keys():
            wordFrequencies[t] += 1
        else:
            wordFrequencies[t] = 1

    return wordFrequencies
    

def updateSubDomains(crawler:crawler, url):
    parsedURL = urlparse(url)
    normalURL = ""
    # returns <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
    if isinstance(parsedURL.netloc, bytes):
        normalURL = normalize(parsedURL.netloc.decode('utf-8')).strip("www.")
    else:
        normalURL = normalize(parsedURL.netloc).strip("www.")

    if "ics.uci.edu" in parsedURL.netloc:
        if normalURL in crawler.icsSubDomainCounts:
            crawler.icsSubDomainCounts[normalURL] += 1
        else:
            crawler.icsSubDomainCounts[normalURL] = 1

    

def updateURLCount(crawler : crawler, url):
    parsedURL = urlparse(url)
    noFragmentURL = normalize(parsedURL.scheme+parsedURL.netloc+parsedURL.path)

    if noFragmentURL not in crawler.uniquePages:
        crawler.uniquePages[noFragmentURL] = True
