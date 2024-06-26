import requests
import cbor
import time

from utils.response import Response
import ssl 


def download(url, config, logger=None):
    # need a check for one of the xml files doesn't have a valid certificate
    # possible security risk but >:)
    ssl._create_default_https_context = ssl._create_unverified_context
    host, port = config.cache_server
    resp = requests.get(
        f"http://{host}:{port}/",
        params=[("q", f"{url}"), ("u", f"{config.user_agent}")], allow_redirects=True)
    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        pass
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
