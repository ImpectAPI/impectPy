from typing import Optional, Dict, Any
from xml.etree import ElementTree as ET

import pandas as pd

from impectPy.config import Config

from .helpers import RateLimitedAPI
from .access_token import getAccessTokenFromUrl
from .iterations import getIterationsFromHost
from .matches import getMatchesFromHost
from .events import getEventsFromHost
from .player_matchsums import getPlayerMatchsumsFromHost
from .squad_matchsums import getSquadMatchsumsFromHost
from .player_iteration_averages import getPlayerIterationAveragesFromHost
from .squad_iteration_averages import getSquadIterationAveragesFromHost
from .player_match_scores import getPlayerMatchScoresFromHost
from .player_iteration_scores import getPlayerIterationScoresFromHost
from .squad_match_scores import getSquadMatchScoresFromHost
from .squad_iteration_scores import getSquadIterationScoresFromHost
from .player_profile_scores import getPlayerProfileScoresFromHost
from .generate_xml import generateXML
from .set_pieces import getSetPiecesFromHost
from .squad_ratings import getSquadRatingsFromHost
from .squad_coefficients import getSquadCoefficientsFromHost
from .formations import getFormationsFromHost
from .substitutions import getSubstitutionsFromHost
from .starting_positions import getStartingPositionsFromHost
from .match_predictions import getMatchPredictionsFromHost
from .data import getDataFromHost


class Impect:
    def __init__(self, config: Optional[Config] = None, connection: Optional[RateLimitedAPI] = None):
        self.__config = config if config is not None else Config()
        self.connection = connection if connection is not None else RateLimitedAPI()

    # login with username and password
    def login(self, username: str, password: str) -> str:
        """Authenticate with the Impect API using username and password and store the access token."""
        self.__token = getAccessTokenFromUrl(username, password, self.connection, self.__config.OIDC_TOKEN_ENDPOINT)
        self.connection.session.headers.update({"Authorization": f"Bearer {self.__token}"})
        return self.__token

    # use the given token for all calls of the instance
    def init(self, token: str):
        """Configure the instance to use the given access token for all subsequent API calls."""
        self.__token = token
        self.connection.session.headers.update({"Authorization": f"Bearer {self.__token}"})

    def getIterations(self) -> pd.DataFrame:
        """Return a DataFrame of all competition iterations available to the authenticated user."""
        return getIterationsFromHost(
            self.connection, self.__config.HOST
        )

    def getMatches(self, iteration: int) -> pd.DataFrame:
        """Return a DataFrame of all matches for the given iteration."""
        return getMatchesFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getEvents(self, matches: list, include_kpis: bool = True, include_set_pieces: bool = True) -> pd.DataFrame:
        """Return a DataFrame of all events for the given list of match IDs."""
        return getEventsFromHost(
            matches, include_kpis, include_set_pieces, self.connection, self.__config.HOST
        )

    def getPlayerMatchsums(self, matches: list) -> pd.DataFrame:
        """Return a DataFrame of per-player KPI sums for the given list of match IDs."""
        return getPlayerMatchsumsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSquadMatchsums(self, matches: list) -> pd.DataFrame:
        """Return a DataFrame of per-squad KPI sums for the given list of match IDs."""
        return getSquadMatchsumsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getPlayerIterationAverages(self, iteration: int) -> pd.DataFrame:
        """Return a DataFrame of per-player KPI averages for the given iteration."""
        return getPlayerIterationAveragesFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getSquadIterationAverages(self, iteration: int) -> pd.DataFrame:
        """Return a DataFrame of per-squad KPI averages for the given iteration."""
        return getSquadIterationAveragesFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getPlayerMatchScores(self, matches: list, positions: list = None) -> pd.DataFrame:
        """Return a DataFrame of per-player scores for the given list of match IDs."""
        return getPlayerMatchScoresFromHost(
            matches, self.connection, self.__config.HOST, positions
        )

    def getPlayerIterationScores(self, iteration: int, positions: list = None) -> pd.DataFrame:
        """Return a DataFrame of per-player iteration-level scores for the given iteration."""
        return getPlayerIterationScoresFromHost(
            iteration, self.connection, self.__config.HOST, positions
        )

    def getSquadMatchScores(self, matches: list) -> pd.DataFrame:
        """Return a DataFrame of per-squad scores for the given list of match IDs."""
        return getSquadMatchScoresFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSquadIterationScores(self, iteration: int) -> pd.DataFrame:
        """Return a DataFrame of per-squad iteration-level scores for the given iteration."""
        return getSquadIterationScoresFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getPlayerProfileScores(self, iteration: int, positions: list) -> pd.DataFrame:
        """Return a DataFrame of per-player profile scores for the given iteration and positions."""
        return getPlayerProfileScoresFromHost(
            iteration, positions, self.connection, self.__config.HOST
        )

    def getSetPieces(self, matches: list) -> pd.DataFrame:
        """Return a DataFrame of all set-piece sub-phases for the given list of match IDs."""
        return getSetPiecesFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSquadRatings(self, iteration: int) -> pd.DataFrame:
        """Return a DataFrame of squad ratings for all dates in the given iteration."""
        return getSquadRatingsFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getSquadCoefficients(self, iteration: int) -> pd.DataFrame:
        """Return a DataFrame of match-prediction model coefficients for the given iteration."""
        return getSquadCoefficientsFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getFormations(self, matches: list) -> pd.DataFrame:
        """Return a DataFrame of all formation changes for the given list of match IDs."""
        return getFormationsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSubstitutions(self, matches: list) -> pd.DataFrame:
        """Return a DataFrame of all substitutions for the given list of match IDs."""
        return getSubstitutionsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getStartingPositions(self, matches: list) -> pd.DataFrame:
        """Return a DataFrame of starting positions for all players in the given list of match IDs."""
        return getStartingPositionsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getMatchPredictions(self, iteration: int) -> pd.DataFrame:
        """Return a DataFrame of match predictions for all matches in the given iteration."""
        return getMatchPredictionsFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getData(
            self, url: str, method: str = "GET", data: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Send an arbitrary API request and return the response as a flattened DataFrame.

        If ``url`` does not start with ``http``, it is treated as a path and prefixed with
        the configured host.
        """
        if not url.startswith("http"):
            url = f"{self.__config.HOST}{url}"
        return getDataFromHost(url=url, method=method, connection=self.connection, data=data)

    @staticmethod
    def generateXML(
            events: pd.DataFrame,
            lead: int,
            lag: int,
            p1Start: int,
            p2Start: int,
            p3Start: int,
            p4Start: int,
            p5Start: int,
            codeTag: str,
            squad: None,
            perspective: None,
            labels=None,
            kpis=None,
            labelSorting: bool = True,
            sequencing: bool = True,
            buckets: bool = True
    ) -> ET.ElementTree:
        """Generate an XML event file for use in video analysis tools and return it as an ElementTree."""
        return generateXML(
            events, lead, lag, p1Start, p2Start, p3Start, p4Start, p5Start, codeTag, squad,
            perspective, labels, kpis, labelSorting, sequencing, buckets
        )