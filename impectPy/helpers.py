# load packages
import requests
import time
import pandas as pd
import re
from typing import Optional, Dict, Any
import math


######
#
# This class creates an object to handle rate-limited API requests
#
######


class RateLimitedAPI:
    def __init__(self, session: Optional[requests.Session] = None):
        """
        Initializes a RateLimitedAPI object.

        Args:
            session (requests.Session): The session object to use for the API calls.
        """
        self.session = session or requests.Session()  # use the provided session or create a new session
        self.bucket = None  # TokenBucket object to manage rate limit tokens

    # make a rate-limited API request
    def make_api_request_limited(self, url: str, method: str, headers: Optional[Dict[str, Any]] = None,
                                 data: Optional[Dict[str, Any]] = None,
                                 json: Optional[Dict[str, Any]] = None) -> requests.Response:
        """
        Executes an API call while applying the rate limit.

        Returns:
            requests.Response: The response returned by the API.
        """

        # check if bucket is not initialized
        if not self.bucket:
            # make an initial API call to get rate limit information
            response = self.make_api_request(url=url, method=method, headers=headers, data=data, json=json)

            # get rate limit policy
            policy = response.headers["RateLimit-Policy"]

            # extract maximum requests using regex
            capacity = int(re.sub(";.*", "", policy))

            # extract time window using regex
            interval = int(re.sub(".*w=(\\d+).*", "\\1", policy))

            # create TokenBucket
            self.bucket = TokenBucket(
                capacity=capacity,
                refill_after=interval,
                remaining=int(response.headers["RateLimit-Remaining"])
            )

            # return response
            return response

        # check if a token is available
        if self.bucket.isTokenAvailable():
            # get API response
            response = self.make_api_request(url=url, method=method, headers=headers, data=data, json=json)

            # consume a token
            self.bucket.consumeToken()
        else:
            # wait for refill
            time.sleep(
                math.ceil(
                    self.bucket.refill_after * 100 - (
                            time.time() - self.bucket.last_refill_time
                    ) *100
                ) / 100
            )

            # call function again
            response = self.make_api_request_limited(url=url, method=method, headers=headers, data=data, json=json)

        # return response
        return response

    def make_api_request(self, url: str, method: str, headers: Optional[Dict[str, Any]] = None,
                         data: Optional[Dict[str, Any]] = None,
                         json: Optional[Dict[str, Any]] = None, max_retries: int = 3,
                         retry_delay: int = 1) -> requests.Response:
        """
        Executes an API call.

        Returns:
            requests.Response: The response returned by the API.
        """
        # try API call
        for i in range(max_retries):
            response = self.session.request(method=method, url=url, headers=headers, data=data, json=json)

            # check status code and return if 200
            if response.status_code == 200:
                # return response
                return response

            # raise exception
            else:
                print(f"Received status code {response.status_code}, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)


######
#
# This class creates a token bucket that handles the rate limit returned by the API accordingly
#
######


class TokenBucket:
    def __init__(self, capacity: int, refill_after: int = 1, remaining: int = 0):
        """
        Initializes a TokenBucket object.

        Args:
            capacity (int): The maximum number of tokens the bucket can hold.
            refill_after (int): The time period (in seconds) after which the bucket is refilled.
            remaining (int): The amount of tokens remaining at the moment of initialization.
        """
        self.capacity = capacity  # maximum number of tokens the bucket can hold
        self.refill_after = refill_after  # time period (in seconds) after which the bucket is refilled
        self.tokens = remaining  # number of tokens remaining at time of bucket creation
        self.last_refill_time = time.time()  # time of the last token refill

    def addTokens(self):
        """
        Refills the token bucket if the refill time has elapsed.
        """
        now = time.time()  # current time
        elapsed_time = now - self.last_refill_time  # time elapsed since the last token refill
        if elapsed_time > self.refill_after:
            self.tokens = self.capacity  # refill the bucket to its maximum capacity
            self.last_refill_time = now  # update the last refill time to the current time

    def isTokenAvailable(self):
        """
        Checks if at least one token is available in the bucket.

        Returns:
            bool: True if a token is available, False otherwise.
        """
        self.addTokens()  # ensure the token bucket is up-to-date
        return self.tokens >= 1  # return True if there is at least one token, False otherwise

    def consumeToken(self):
        """
        Consumes a token from the bucket if available.

        Returns:
            bool: True if a token was consumed successfully, False otherwise.
        """
        if not self.isTokenAvailable():  # if no token is available, return False
            return False
        self.tokens -= 1  # decrement the token count by 1
        return True  # return True to indicate successful token consumption


######
#
# This function converts the response from an API call to a pandas dataframe, flattens it and fixes the column names
#
######


def process_response(self: requests.Response) -> pd.DataFrame:
    # get data from response
    result = self.json()["data"]

    # convert to df
    result = pd.json_normalize(result)

    # fix column names using regex
    result = result.rename(columns=lambda x: re.sub("\.(.)", lambda y: y.group(1).upper(), x))

    # return result
    return result


# attach method to requests module
requests.Response.process_response = process_response


######
#
# This function unnests the idMappings key from an API response
#
######


def unnest_mappings(dict: dict) -> dict:
    # iterate over iterations and unnest idMappings
    for iteration in dict:
        # iterate over mappings
        for mapping in iteration["idMappings"]:
            # get mapping data
            for provider, id in mapping.items():
                # add mapping as key on iteration level
                iteration[provider + "Id"] = id

    # return result
    return dict

