import time
from scrapy.downloadermiddlewares.retry import RetryMiddleware

class CustomRetryMiddleware(RetryMiddleware):
    def process_response(self, request, response, spider):
        if response.status == 429:
            # Sleep for a certain period of time before retrying the request
            time.sleep(10)  # Sleep for 60 seconds (adjust as needed)
            
            # Retry the request after the delay
            retryreq = self._retry(request, "HTTP 429 Error")
            return retryreq or response
        return response
