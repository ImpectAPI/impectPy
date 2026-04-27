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
        url: str, token: str, method: str = "GET", data: Optional[Dict[str, Any]] = None,
        session: Optional[ImpectSession] = None
) -> pd.DataFrame:
    """Authenticate and call any Impect API endpoint, returning the response as a DataFrame."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session or ImpectSession())

    # set auth header
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getDataFromHost(url=url, method=method, connection=connection, data=data)


def getDataFromHost(
        url: str, method: str, connection: RateLimitedAPI, data: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Execute a rate-limited API call to the given URL and return the response as a DataFrame."""
    response = connection.make_api_request_limited(url=url, method=method, data=data)
    return response.process_response(endpoint=url)
