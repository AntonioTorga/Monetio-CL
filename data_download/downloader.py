from utils.webclient import WebClient
class Downloader:
    def __init__(self):
        self.url = None

    def set_url(self, url: str):
        self.url = url
    
    def get(self, endpoint: str):
        client = WebClient(self.url)
        return client.get(endpoint)
