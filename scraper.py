import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import crawler
import shelve
from utils import get_logger, get_urlhash, normalize


stopWords = {"we'd", 'his', "you're", 'its', "mustn't", "i'd", "you've", 'that', 'nor', 'only', 'both', 'because', 'through', 'from', 'herself', 'same', 'themselves', 'having', 'this', "we're", 'further', 'your', 'which', "that's", 'down', 'been', 'more', "weren't", 'why', 'with', 'some', 'them', 'below', 'their', "couldn't", 'if', 'then', 'in', 'about', 'i', 'of', "wouldn't", "she's", 'all', "i'm", 'than', 'what', 'when', 'against', 'so', 'he', 'did', "hadn't", 'those', "aren't", 'here', 'yours', "it's", 'be', 'until', "when's", 'no', 'an', "don't", 'not', 'were', "doesn't", 'me', 'on', "there's", 'at', 'any', 'out', "i've", 'over', 'have', 'has', 'we', "they've", "wasn't", "we'll", 'yourselves', 'whom', "hasn't", "they'll", 'a', 'to', 'but', "he'd", 'am', 'her', 'above', 'under', 'the', 'after', "they'd", 'doing', "haven't", 'should', 'him', 'is', 'other', "shouldn't", 'how', 'cannot', 'they', "i'll", 'itself', 'myself', 'himself', 'between', 'it', 'would', 'my', "they're", "she'll", 'ours', 'or', 'was', 'where', "won't", "can't", 'too', "here's", "where's", 'again', 'into', 'most', "let's", 'does', 'by', 'being', 'these', 'such', "he'll", "isn't", "didn't", "who's", 'few', "you'd", 'you', 'do', 'each', 'ourselves', "we've", 'yourself', 'who', 'during', 'our', 'are', "what's", "you'll", 'and', 'as', 'hers', 'once', 'up', 'off', "shan't", 'she', 'there', 'while', "he's", 'could', "how's", 'very', 'before', 'ought', 'for', 'had', "she'd", "why's", 'own', 'theirs'}
wordCountThreshold = 100
contentToCodeRatioThreshold = 0.9
uniqueWordRatioThreshold = 0.02
linkToContentRatioThreshold = 10


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    hyperlinkList = []
    if resp.status == 200:

        # Parse the page content using beautiful soup
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')

        # Check for low information pages
        if not checkLowInfo(soup, resp.url):
            return hyperlinkList

        # Iterate through <a> objects, adding the hyperlink to list
        for link in soup.find_all('a'):
            hyperlinkList.append(link.get('href'))
        
    else:
        print(f"Failed to retrieve the web page. Status code: {resp.status}. Error code: {resp.error}.")

    return hyperlinkList

def checkLowInfo(soup, url):
    # Check word count
    totalWords = len(soup.get_text())
    if totalWords < wordCountThreshold:
        get_logger("CRAWLER").warning(f"low total words on {url}")
        return False
    
    # Check Content to Code Ratio
    HTMLCSSJSCount = len(soup.find_all(['html', 'head', 'meta', 'link', 'script', 'style']))
    paragraphCount = len(soup.find_all('p'))
    linkCount = len(soup.find_all('a'))
    total_elements = HTMLCSSJSCount + paragraphCount + linkCount

    if total_elements == 0:
        get_logger("CRAWLER").warning(f"0 total_elements count on {url}")
        return False
    if (HTMLCSSJSCount / total_elements) > contentToCodeRatioThreshold:
        get_logger("CRAWLER").warning(f"high html count of {HTMLCSSJSCount / total_elements} on {url}")
        return False

    # Check for low number of unique words
    uniqueWords = re.findall(r'\b\w+\b', soup.get_text().lower())
    uniqueWordsCount = len(set(uniqueWords))
    
    if (uniqueWordsCount / totalWords) < uniqueWordRatioThreshold: # Total words guaranteed to be above 0 due to word count check
        get_logger("CRAWLER").warning(f"low unique words of {uniqueWordsCount / totalWords} on {url}")
        return False

    #I would think we want a high amount of links?
    #Check link-to-text ratio
    # if paragraphCount == 0:
    #    get_logger("CRAWLER").warning(f"0 paragraph count on {url}")
    #    return False
    if linkCount / (total_elements - linkCount) > linkToContentRatioThreshold:
       get_logger("CRAWLER").warning(f"high link to paragraph ratio of {linkCount / (total_elements - linkCount)} on {url}")
       return False
    
    return True

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
       
        parsed = urlparse(url) # returns <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
        parsedNetloc = parsed.netloc
        if isinstance(parsedNetloc, bytes):
            parsedNetloc = parsed.netloc.decode('utf-8')  
            
        if not checkDomain(parsedNetloc):
            return False

        if parsed.scheme not in set(["http", "https"]):
            return False

        if not checkDuplicate(url):
            return False
        
        pattern = r"(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|thmx|mso|arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz)"

        compiled_pattern = re.compile(pattern, re.IGNORECASE)

        # Search for the pattern anywhere in the path
        return not compiled_pattern.search(parsed.path.lower())


    except TypeError:
        print ("TypeError for ", parsed)
        raise

def checkDomain(netloc: str) -> bool:
    # string netloc: Authority aspect of a URL: <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
    # Return whether netloc is a valid domain

    # Available Domains
    domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]

    parsed = netloc # Is there a reason to make a copy of it?

    # Sometimes netloc can be in bytes, ifso, decode to string
    if isinstance(netloc, bytes):
       parsed = netloc.decode('utf-8')  
    

    for domain in domains:
        if domain in parsed:
            return True
    
    # If netloc does not match any valid domains, return false
    return False

def checkDuplicate(url):
    # For exact duplicates, use hash

    # For near similar duplicates, use fingerprints
    # Simple fingerprint: parse the document into words
    # Group tokens into contiguous n-gams for some n.
    # Some of the n-grams are selected to represent the document
    # Use hash function for each n-gram to transform into integers
    # Use modulus operator on hash values and store those values as the "fingerprint"
    # Calculate the intersection between this urls fingerprint and other fingerprints
    # Use designed threshold to compare if two fingerprints are too similar

    return True

def updateTokens(crawler : crawler, resp):
    if resp.status == 200:

        # Parse the page content using beautiful soup
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')

        # Iterate and read through each <p> within the text content
        text = ""
        for paragraph in soup.find_all("p"):
            text += paragraph.get_text() + " "
        
        # Take all text and tokenize it
        text = tokenize_text(text)

        # remove stopwords and punctuation
        tokens = [t for t in text if t not in stopWords]
        
        # update the longest 
        if(len(tokens) > crawler.longest):
            with crawler.longestLock:
                crawler.longestFile.clear() 
                crawler.longestFile[normalize(resp.url)] = len(tokens)
                crawler.longest = len(tokens)
                crawler.longestFile.sync()
            

        # build frequency dict
        tokens = computeWordFrequencies(tokens)

        with crawler.tokensLock:
            # update counts
            for k,v in tokens.items():
                if k in crawler.tokens:
                    crawler.tokens[k] += v
                else:
                    crawler.tokens[k] = v
            crawler.tokens.sync()
    else:
        print(f"Failed to retrieve the web page. Status code: {resp.status}. Error code: {resp.error}.")


def tokenize_text(text):
    tokens = []
    current_token = []
    for char in text:
        if (char.isascii() and char.isalnum()) or (char == "'" or char == "-"):
                current_token.append(char)
        else:
            if current_token:
                tokens.append(''.join(current_token).lower())
                current_token = []
    # Place token on if it exists
    if current_token:
        tokens.append(''.join(current_token).lower())
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

    if "ics.uci.edu" in normalURL and "ics.uci.edu" != normalURL:
        # reserve the ics domain lock
        with crawler.icsSubDomainCountsLock:
            if normalURL in crawler.icsSubDomainCounts:
                crawler.icsSubDomainCounts[normalURL] += 1
            else:
                crawler.icsSubDomainCounts[normalURL] = 1

            crawler.icsSubDomainCounts.sync()
    

def updateURLCount(crawler : crawler, url):
    parsedURL = urlparse(url)
    noFragmentURL = normalize(parsedURL.scheme+parsedURL.netloc+parsedURL.path)

    with crawler.uniquePagesLock:
        if noFragmentURL not in crawler.uniquePages:
            crawler.uniquePages[noFragmentURL] = True
            crawler.uniquePages.sync()


def checkUniqueNetloc(crawler : crawler, url):
    parsed = urlparse(url)

    with crawler.netlocsLock:
        if normalize(parsed.netloc) in crawler.netlocs:
            return False
        else:
            crawler.netlocs[normalize(parsed.netloc)] = True
            crawler.netlocs.sync()