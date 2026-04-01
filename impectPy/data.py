# load packages
import pandas as pd
from typing import Optional, Dict, Any
from impectPy.helpers import RateLimitedAPI, ImpectSession


######
#
# This function returns a dataframe from any Impect API endpoint
#
######


def getData(
        url: str, token: str, method: str = "GET", data: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Returns a processed DataFrame from any Impect API endpoint.

    Args:
        url (str): Full URL of the API endpoint.
        token (str): Bearer token for authentication.
        method (str): HTTP method. Defaults to "GET".
        data (Optional[Dict[str, Any]]): Optional request body. Defaults to None.

    Returns:
        pd.DataFrame: Processed response data.
    """
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(ImpectSession())

    # set auth header
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getDataFromHost(url=url, method=method, connection=connection, data=data)


def getDataFromHost(
        url: str, method: str, connection: RateLimitedAPI, data: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Core implementation: executes a rate-limited API call and returns a processed DataFrame.

    Args:
        url (str): Full URL of the API endpoint.
        method (str): HTTP method.
        connection (RateLimitedAPI): Authenticated connection object.
        data (Optional[Dict[str, Any]]): Optional request body. Defaults to None.

    Returns:
        pd.DataFrame: Processed response data.
    """
    response = connection.make_api_request_limited(url=url, method=method, data=data)
    return response.process_response(endpoint=url)
