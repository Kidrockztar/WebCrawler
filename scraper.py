import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import crawler
from collections import Counter
from utils import get_logger, get_urlhash, normalize
from utils.download import download


stopWords = {"we'd", 'his', "you're", 'its', "mustn't", "i'd", "you've", 'that', 'nor', 'only', 'both', 'because', 'through', 'from', 'herself', 'same', 'themselves', 'having', 'this', "we're", 'further', 'your', 'which', "that's", 'down', 'been', 'more', "weren't", 'why', 'with', 'some', 'them', 'below', 'their', "couldn't", 'if', 'then', 'in', 'about', 'i', 'of', "wouldn't", "she's", 'all', "i'm", 'than', 'what', 'when', 'against', 'so', 'he', 'did', "hadn't", 'those', "aren't", 'here', 'yours', "it's", 'be', 'until', "when's", 'no', 'an', "don't", 'not', 'were', "doesn't", 'me', 'on', "there's", 'at', 'any', 'out', "i've", 'over', 'have', 'has', 'we', "they've", "wasn't", "we'll", 'yourselves', 'whom', "hasn't", "they'll", 'a', 'to', 'but', "he'd", 'am', 'her', 'above', 'under', 'the', 'after', "they'd", 'doing', "haven't", 'should', 'him', 'is', 'other', "shouldn't", 'how', 'cannot', 'they', "i'll", 'itself', 'myself', 'himself', 'between', 'it', 'would', 'my', "they're", "she'll", 'ours', 'or', 'was', 'where', "won't", "can't", 'too', "here's", "where's", 'again', 'into', 'most', "let's", 'does', 'by', 'being', 'these', 'such', "he'll", "isn't", "didn't", "who's", 'few', "you'd", 'you', 'do', 'each', 'ourselves', "we've", 'yourself', 'who', 'during', 'our', 'are', "what's", "you'll", 'and', 'as', 'hers', 'once', 'up', 'off', "shan't", 'she', 'there', 'while', "he's", 'could', "how's", 'very', 'before', 'ought', 'for', 'had', "she'd", "why's", 'own', 'theirs'}
wordCountThreshold = 100
contentToCodeRatioThreshold = 0.9
uniqueWordRatioThreshold = 0.02
linkToContentRatioThreshold = 20
simHashThreshold = 0.8


def scraper(crawler : crawler, url, resp): 
    links = extract_next_links(crawler, url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(crawler, url, resp):
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

        # Check for duplicates/near duplicates
        if not checkDuplicate(crawler, soup, resp):
            print(f"returning after checking hash for {resp.url}")
            return []

        # Check for low information pages
        if not checkLowInfo(crawler, soup, resp.url):
            return []

        # Iterate through <a> objects, adding the hyperlink to list
        for link in soup.find_all('a'):
            hyperlinkList.append(urljoin(resp.url, link.get('href')))
        
        for url in soup.find_all('url'):
            hyperlinkList.append(urljoin(resp.url, url.get('href')))
        
    else:
        print(f"Failed to retrieve the web page. Status code: {resp.status}. Error code: {resp.error}.")

    return hyperlinkList

def checkLowInfo(crawler, soup, url):

    # Check for non html webpages
    pattern = r".*\.(r|txt|bib)$"
    if re.match(pattern, url):
        return True

    # Check word count
    totalWords = len(soup.get_text())
    if totalWords < wordCountThreshold:
        finalURL = handleRedirects(crawler, url)
        if (finalURL):
            print("Redirected url:", finalURL)
        crawler.logger.warning(f"low total words on {url}")
        return False
    
    # Check Content to Code Ratio
    HTMLCSSJSCount = len(soup.find_all(['html', 'head', 'meta', 'link', 'script', 'style']))
    paragraphCount = len(soup.find_all('p'))
    linkCount = len(soup.find_all('a'))
    total_elements = HTMLCSSJSCount + paragraphCount + linkCount

    if total_elements == 0:
        crawler.logger.warning(f"0 total_elements count on {url}")
        return False
    
    if (HTMLCSSJSCount / total_elements) > contentToCodeRatioThreshold:
        crawler.logger.warning(f"high html count of {HTMLCSSJSCount / total_elements} on {url}")
        return False

    # Check for low number of unique words
    uniqueWords = re.findall(r'\b\w+\b', soup.get_text().lower())
    uniqueWordsCount = len(set(uniqueWords))
    
    if (uniqueWordsCount / totalWords) < uniqueWordRatioThreshold: # Total words guaranteed to be above 0 due to word count check
        crawler.logger.warning(f"low unique words of {uniqueWordsCount / totalWords} on {url}")
        return False

    #I would think we want a high amount of links?
    #Check link-to-text ratio
    pageWithoutLinks = total_elements - linkCount
    if pageWithoutLinks == 0:
       crawler.logger.warning(f"0 content count on {url}")
       return False
    
    if linkCount / pageWithoutLinks > linkToContentRatioThreshold:
       crawler.logger.warning(f"high link to content ratio of {linkCount / pageWithoutLinks} on {url}")
       return False
    
    return True

def handleRedirects(crawler, url):
    try:
        resp = download(url, crawler.frontier.config)
        for response in resp.history:
            print("REPONSE HISTORY: ", response.status_code, response.url)

    except Exception as e:
        print(f"Error occurred while fetching final URL: {e}")
        return None

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

def checkDuplicate(crawler: crawler, soup, resp):
    # For exact duplicates, use hash
    totalText = soup.get_text()
    crcHash = cyclic_redundancy_check(totalText)

    # Reserve dict for this thread
    with crawler.hashOfPagesLock:
        # need to be str so the key lookup works
        if str(crcHash) in crawler.hashOfPages:
            crawler.logger.warning(f"hash: {crcHash} already visited")
            return False
        else:
            crawler.hashOfPages[str(crcHash)] = True

    # Check for near duplicates
    with crawler.simHashSetLock:
        sim_hash = simHash(totalText)
        for sim in crawler.simHashSet.keys():
            sim = int(sim)
            if areSimilarSimHashes(sim_hash, sim, simHashThreshold):
                crawler.logger.warning(f"high similarity on {resp.url}")
                return False

        crawler.simHashSet[str(sim_hash)] = True

    return True

def cyclic_redundancy_check(pageData):
    
    crcHash = 0xFFFF

    # Convert page data into bytes and iterate
    for byte in pageData.encode():

        crcHash ^= byte # bitwise XOR

        for _ in range(8): # 8 bits in a byte
            if crcHash & 0x0001: # Check LSB 
                crcHash = (crcHash >> 1) ^ 0xA001 # Polynomial divison process
            else:
                crcHash >>= 1 # Shift right by 1 bit to discard LSB

    # Return compliment of hash
    return crcHash ^ 0xFFFF

def simHash(pageData):
    # Seperate into words with weights
    weightedWords = Counter(tokenize_text(pageData))

    # Get 8-bit hash values for every unique word
    hashValues = {word: bit_hash(word) for word in weightedWords}

    # Calculate the Vector V by summing weights
    simhash_value = [0] * 8

    for word, count in weightedWords.items():
        wordHash = hashValues[word]

        for i in range(8):
            # Offset hash digit by index of range, and multiply by 1 to get LSB
            bit = (wordHash >> i) & 1
            simhash_value[i] += (1 if bit else -1) * count
    
    # Convert into fingerprint
    simhash_fingerprint = 0
    for i in range(8):
        if simhash_value[i] > 0:
            simhash_fingerprint |= 1 << i
    
    return simhash_fingerprint

def bit_hash(word):

    # Function to generate an 8 bit hash for a word
    hash = 0

    for character in word:
        # Add ASCII value to total hash
        hash += ord(character)
    
    # Ensure hash value is 8 bits
    return hash % 256

def areSimilarSimHashes(firstSimHash, secondSimHash, threshold):
    # Return true if two hashes are similar, else false

    # Get number of different bits by XOR the two hashes, and count the occurances of 0 (similarities)
    similarBits = bin(firstSimHash ^ secondSimHash).count('0')
    # Calculate the similarity ratio
    similarity = similarBits / 8

    #if similarity >= threshold:
    #    get_logger("CRAWLER").warning(f"simHash similarity: {similarity} already visited")

    return similarity >= threshold

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
        with crawler.longestLock:
            if(len(tokens) > crawler.longest.values()[0]):
                crawler.logger.info(f"Updating longest with {len(tokens)} and url : {resp.url}")
                crawler.longestFile.clear() 
                crawler.longestFile[normalize(resp.url)] = len(tokens)
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
            return True