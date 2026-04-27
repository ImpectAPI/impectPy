# load packages
import numpy as np
import requests
import time
import pandas as pd
import re
import math
import logging
import warnings
from typing import Optional, Dict, Any, NamedTuple

# create logger for this module
logger = logging.getLogger("impectPy")
logger.addHandler(logging.NullHandler())


######
#
# This class inherits from Response and adds a function converts the response from an API call to a pandas dataframe, flattens it and fixes the column names
#
######

class ImpectResponse(requests.Response):
    def process_response(self, endpoint: str, raise_exception: bool = True) -> pd.DataFrame:
        """Validate the API response, flatten the JSON data, and return it as a DataFrame."""
        # validate and get data from response
        result = validate_response(response=self, endpoint=endpoint, raise_exception=raise_exception)

        # convert to df
        result = pd.json_normalize(result)

        # fix column names using regex
        result = result.rename(columns=lambda x: re.sub(r"\.(.)", lambda y: y.group(1).upper(), x))

        # return result
        return result


######
#
# This class inherits from Session and ensure the response is of type ImpectResponse
#
######

class ImpectSession(requests.Session):
    def request(self, *args, **kwargs) -> ImpectResponse:
        """Send a request and return the response cast to ImpectResponse."""
        response = super().request(*args, **kwargs)
        response.__class__ = ImpectResponse
        return response


######
#
# This class creates an object to handle rate-limited API requests
#
######


class HTTPError(Exception):
    """Raised when the API returns status code other than 200 or 403."""
    pass


class ForbiddenError(HTTPError):
    """Raised when the API returns a 403 Forbidden response."""
    pass


class RateLimitedAPI:
    def __init__(self, session: Optional[ImpectSession] = None):
        """Initialize a RateLimitedAPI instance, using the provided session or a new ImpectSession."""
        self.session = session or ImpectSession()  # use the provided session or create a new session
        self.bucket = None  # TokenBucket object to manage rate limit tokens

    # make a rate-limited API request
    def make_api_request_limited(
            self, url: str, method: str, data: Optional[Dict[str, str]] = None
    ) -> ImpectResponse:
        """Execute a rate-limited API call and return the response."""

        # check if bucket is not initialized
        if not self.bucket:
            # make an initial API call to get rate limit information
            response = self.make_api_request(url=url, method=method, data=data)

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

            return response

        # check if a token is available
        if self.bucket.isTokenAvailable():
            # get API response
            response = self.make_api_request(url=url, method=method, data=data)

            # consume a token
            self.bucket.consumeToken()
        else:
            # wait for refill
            time.sleep(
                math.ceil(
                    self.bucket.refill_after * 100 - (
                            time.time() - self.bucket.last_refill_time
                    ) * 100
                ) / 100
            )

            # call function again
            response = self.make_api_request_limited(url=url, method=method, data=data)

        # return response
        return response

    def make_api_request(
            self, url: str, method: str, data: Optional[Dict[str, Any]] = None,
            max_retries: int = 3, retry_delay: Optional[int] = None
    ) -> ImpectResponse:
        """Execute an API call with retries and return the response."""
        # try API call
        for i in range(max_retries):
            response = self.session.request(method=method, url=url, data=data)

            # check status code and return if 200
            if response.status_code == 200:
                # return response
                return response
            # check status code and retry if 429
            elif response.status_code == 429:
                # check if last try
                if i < max_retries - 1:
                    # calculate exact wait time based on token bucket refill time
                    if self.bucket and retry_delay is None:
                        wait_time = max(0, math.ceil(
                            self.bucket.refill_after * 100 - (
                                    time.time() - self.bucket.last_refill_time
                            ) * 100
                        ) / 100)
                    else:
                        wait_time = retry_delay if retry_delay is not None else 1

                    print(f"Received status code {response.status_code} "
                          f"({response.json().get('message', 'Rate Limit Exceeded')})"
                          f", retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise HTTPError(f"Received status code {response.status_code} "
                                    f"({response.json().get('message', 'Rate Limit Exceeded')})"
                                    f", exceeded maximum number of {max_retries} retries.")
            # check status code and terminate if 401 or 403
            elif response.status_code == 401:
                exception_message = f"Received status code {response.status_code} (Invalid User Credentials)."
                if "x-request-id" in response.headers:
                    exception_message += (f" Request-ID: {response.headers['x-request-id']} "
                                          f"(Make sure to include this in any support request.)")

                raise HTTPError(exception_message)
            elif response.status_code == 403:
                raise ForbiddenError(f"Received status code {response.status_code} "
                                     f"(You do not have access to this resource.). "
                                     f"Request-ID: {response.headers['x-request-id']} "
                                     f"(Make sure to include this in any support request.)")
            # check status code and terminate if other error
            else:
                raise HTTPError(f"Received status code {response.status_code} "
                                f"({response.json().get('message', 'Unknown error')}). "
                                f"Request-ID: {response.headers['x-request-id']} "
                                f"(Make sure to include this in any support request.)")


######
#
# This class creates a token bucket that handles the rate limit returned by the API accordingly
#
######


class TokenBucket:
    def __init__(self, capacity: int, refill_after: int = 1, remaining: int = 0):
        """Initialize a TokenBucket with the given capacity, refill interval, and initial token count."""
        self.capacity = capacity  # maximum number of tokens the bucket can hold
        self.refill_after = refill_after  # time period (in seconds) after which the bucket is refilled
        self.tokens = remaining  # number of tokens remaining at time of bucket creation
        self.last_refill_time = time.time()  # time of the last token refill

    def addTokens(self):
        """Refill the token bucket to capacity if the refill interval has elapsed."""
        now = time.time()  # current time
        elapsed_time = now - self.last_refill_time  # time elapsed since the last token refill
        if elapsed_time > self.refill_after:
            self.tokens = self.capacity  # refill the bucket to its maximum capacity
            self.last_refill_time = now  # update the last refill time to the current time

    def isTokenAvailable(self):
        """Return True if at least one token is available in the bucket, False otherwise."""
        self.addTokens()  # ensure the token bucket is up-to-date
        return self.tokens >= 1  # return True if there is at least one token, False otherwise

    def consumeToken(self):
        """Consume one token from the bucket and return True, or return False if none are available."""
        if not self.isTokenAvailable():  # if no token is available, return False
            return False
        self.tokens -= 1  # decrement the token count by 1
        return True  # return True to indicate successful token consumption


######
#
# This function unnests the idMappings key from an API response
#
######


def unnest_mappings_dict(mapping_dict: dict) -> dict:
    """Unnest the idMappings entries in a list of dicts and return the modified list.

    Each item's ``idMappings`` list is iterated and each provider key is promoted to a
    top-level key of the form ``<provider>Id``.
    """
    # iterate over entry and unnest idMappings
    for entry in mapping_dict:
        # iterate over mappings
        for mapping in entry["idMappings"]:
            # get mapping data
            for provider, mapping_id in mapping.items():
                # add mapping as key on iteration level
                entry[provider + "Id"] = mapping_id

    # return result
    return mapping_dict


######
#
# This function unnests the idMappings key from a dataframe
#
######


def unnest_mappings_df(df: pd.DataFrame, mapping_col: str) -> pd.DataFrame:
    """Unnest the idMappings column of a DataFrame and return the expanded DataFrame.

    Reads the list of provider-to-ID mappings stored in ``mapping_col``, normalises
    provider names, and concatenates the resulting columns alongside the original DataFrame.
    """
    # create empty df to store mappings
    df_mappings = pd.DataFrame(columns=["wyscoutId", "heimSpielId", "skillCornerId", "optaId", "statsPerformId", "transfermarktId", "soccerdonnaId"])

    # iterate over entry and unnest idMappings
    for index, entry in df.iterrows():
        # iterate over mappings
        for mapping in entry[mapping_col]:
            # get mapping data
            for provider, mapping_ids in mapping.items():
                # fix provider name
                if provider == "heim_spiel":
                    provider = "heimSpiel"
                elif provider == "skill_corner":
                    provider = "skillCorner"
                elif provider == "stats_perform":
                    provider = "statsPerform"
                elif provider in ("wyscout", "opta", "transfermarkt", "soccerdonna"):
                    pass
                else:
                    raise Exception(f"Unknown provider: {provider}")

                # check if mapping is a dict with at least one entry
                if isinstance(mapping_ids, list):
                    if len(mapping_ids) > 0:
                        # add first mapping as key on iteration level
                        df_mappings.loc[index, provider + "Id"] = mapping_ids[0]
                else:
                    df_mappings.loc[index, provider + "Id"] = np.nan

    # merge with original df
    df = pd.concat([df, df_mappings], axis=1, ignore_index=False)

    # return result
    return df


######
#
# This function validates the response from an API call and returns the data
#
######


# define function to validate JSON response and return data
def validate_response(response: ImpectResponse, endpoint: str, raise_exception: bool = True) -> dict:
    """Validate the JSON response from an API call and return the data payload.

    Raises an exception when the data list is empty and ``raise_exception`` is True.
    """
    # get data from response
    data = response.json()["data"]

    # check if response contains data
    if len(data) == 0 and raise_exception:
        # raise exception
        raise Exception(f"The {endpoint} endpoint returned no data/ an empty list.")
    else:
        # return data
        return data


######
#
# This function wraps any function to safely execute it and return a fallback if it fails
#
######


def safe_execute(func, *args, fallback=None, identifier: str, forbidden_list: list, **kwargs):
    """Execute func safely and return its result, or fallback if an exception occurs.

    Logs errors on failure, appends the identifier to forbidden_list on HTTP 403, and
    returns an empty DataFrame as the default fallback.
    """
    if fallback is None:
        fallback = pd.DataFrame()
    try:
        return func(*args, **kwargs)
    except HTTPError as e:
        logger.error(
            f"Request failed: {e}",
            extra={
                "id": identifier,
                "url": kwargs.get("url"),
            },
            # exc_info=not isinstance(e, HTTPError)
        )
        if isinstance(e, ForbiddenError):
            forbidden_list.append(identifier)
        return fallback


######
#
# This NamedTuple holds the result of resolving a list of match IDs
#
######


class MatchResolution(NamedTuple):
    match_data: pd.DataFrame  # full match info for all valid matches
    matches: list             # filtered match IDs (forbidden + unavailable removed)
    iterations: list          # unique iteration IDs from valid matches


######
#
# This function validates a list of match IDs and returns match data, a
# filtered match list, and the unique iteration IDs
#
######


def resolve_matches(matches: list, connection: RateLimitedAPI, host: str) -> MatchResolution:
    """Validate a list of match IDs and return their metadata, filtered IDs, and iteration IDs.

    Fetches match info for each ID in ``matches``, removes forbidden and unavailable matches
    with appropriate warnings, and returns a MatchResolution named tuple containing the full
    match DataFrame, the filtered match ID list, and the unique iteration IDs.
    """
    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Argument 'matches' must be a list of integers.")

    # create list to store matches that are forbidden (HTTP 403)
    forbidden_matches = []

    # create list to store dfs
    def fetch_match_info(conn, url):
        return conn.make_api_request_limited(
            url=url, method="GET"
        ).process_response(endpoint="Match Info")

    match_data_list = []
    for match in matches:
        match_data = safe_execute(
            fetch_match_info,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}",
            identifier=match,
            forbidden_list=forbidden_matches
        )
        match_data_list.append(match_data)

    # drop empty responses and raise if none remain
    match_data_list = [df for df in match_data_list if not df.empty]
    if not match_data_list:
        raise Exception("All supplied matches are unavailable or forbidden. Execution stopped.")
    match_data = pd.concat(match_data_list)

    # filter for matches that are unavailable
    unavailable_matches = match_data[match_data.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in unavailable_matches]

    # drop matches that are forbidden
    matches = [match for match in matches if match not in forbidden_matches]

    # configure warning format
    def no_line_formatter(message, category, filename, lineno, line):
        return f"Warning: {message}\n"
    warnings.formatwarning = no_line_formatter

    # raise exception if no matches remaining or report removed matches
    if len(matches) == 0:
        raise Exception("All supplied matches are unavailable or forbidden. Execution stopped.")
    if len(forbidden_matches) > 0:
        warnings.warn(f"The following matches are forbidden for the user: {forbidden_matches}")
    if len(unavailable_matches) > 0:
        warnings.warn(f"The following matches are not available yet and were ignored: {unavailable_matches}")

    # extract iterationIds
    iterations = list(match_data[match_data.lastCalculationDate.notnull()].iterationId.unique())

    return MatchResolution(match_data=match_data, matches=matches, iterations=iterations)
