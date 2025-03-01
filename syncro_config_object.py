class SyncroConfig:
    def __init__(self, subdomain: str, api_key: str):
        self.subdomain = subdomain
        self.api_key = api_key
        self.base_url = f"https://{subdomain}.syncromsp.com/api/v1"
