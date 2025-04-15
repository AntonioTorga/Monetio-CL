class WebClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def get(self, endpoint: str):
        # Simulate a GET request
        print(f"GET request to {self.base_url}{endpoint}")
        return {"data": "sample data"}

    def post(self, endpoint: str, data: dict):
        # Simulate a POST request
        print(f"POST request to {self.base_url}{endpoint} with data {data}")
        return {"status": "success"}