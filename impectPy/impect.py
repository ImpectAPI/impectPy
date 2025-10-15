from impectPy.config import Config
from .helpers import RateLimitedAPI
from .access_token import getAccessTokenFromUrl
from .iterations import getIterationsFromHost
from .matches import getMatchesFromHost
from .events import getEventsFromHost
from .matchsums import getPlayerMatchsumsFromHost, getSquadMatchsumsFromHost
from .iteration_averages import getPlayerIterationAveragesFromHost, getSquadIterationAveragesFromHost
from .player_scores import getPlayerMatchScoresFromHost, getPlayerIterationScoresFromHost
from .squad_scores import getSquadMatchScoresFromHost, getSquadIterationScoresFromHost
from .player_profile_scores import getPlayerProfileScoresFromHost
from .xml import generateXML
from .set_pieces import getSetPiecesFromHost
from .squad_ratings import getSquadRatingsFromHost
from .squad_coefficients import getSquadCoefficientsFromHost
from .match_info import getFormationsFromHost, getSubstitutionsFromHost, getStartingPositionsFromHost
import pandas as pd
from xml.etree import ElementTree as ET


class Impect:
    def __init__(self, config: Config = Config(), connection: RateLimitedAPI = RateLimitedAPI()):
        self.__config = config
        self.connection = connection

    # login with username and password
    def login(self, username: str, password: str) -> str:
        self.__token = getAccessTokenFromUrl(username, password, self.connection, self.__config.OIDC_TOKEN_ENDPOINT)
        self.connection.session.headers.update({"Authorization": f"Bearer {self.__token}"})
        return self.__token

    # use the given token for all calls of the instance
    def init(self, token: str):
        self.__token = token
        self.connection.session.headers.update({"Authorization": f"Bearer {self.__token}"})

    def getIterations(self) -> pd.DataFrame:
        return getIterationsFromHost(
            self.connection, self.__config.HOST
        )

    def getMatches(self, iteration: int) -> pd.DataFrame:
        return getMatchesFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getEvents(self, matches: list, include_kpis: bool = True, include_set_pieces: bool = True) -> pd.DataFrame:
        return getEventsFromHost(
            matches, include_kpis, include_set_pieces, self.connection, self.__config.HOST
        )

    def getPlayerMatchsums(self, matches: list) -> pd.DataFrame:
        return getPlayerMatchsumsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSquadMatchsums(self, matches: list, ) -> pd.DataFrame:
        return getSquadMatchsumsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getPlayerIterationAverages(self, iteration: int) -> pd.DataFrame:
        return getPlayerIterationAveragesFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getSquadIterationAverages(self, iteration: int) -> pd.DataFrame:
        return getSquadIterationAveragesFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getPlayerMatchScores(self, matches: list, positions: list = None) -> pd.DataFrame:
        return getPlayerMatchScoresFromHost(
            matches, self.connection, self.__config.HOST, positions
        )

    def getPlayerIterationScores(self, iteration: int, positions: list = None) -> pd.DataFrame:
        return getPlayerIterationScoresFromHost(
            iteration, self.connection, self.__config.HOST, positions
        )

    def getSquadMatchScores(self, matches: list) -> pd.DataFrame:
        return getSquadMatchScoresFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSquadIterationScores(self, iteration: int) -> pd.DataFrame:
        return getSquadIterationScoresFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getPlayerProfileScores(self, iteration: int, positions: list) -> pd.DataFrame:
        return getPlayerProfileScoresFromHost(
            iteration, positions, self.connection, self.__config.HOST
        )

    def getSetPieces(self, matches: list) -> pd.DataFrame:
        return getSetPiecesFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSquadRatings(self, iteration: int) -> pd.DataFrame:
        return getSquadRatingsFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getSquadCoefficients(self, iteration: int) -> pd.DataFrame:
        return getSquadCoefficientsFromHost(
            iteration, self.connection, self.__config.HOST
        )

    def getFormations(self, matches: list) -> pd.DataFrame:
        return getFormationsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getSubstitutions(self, matches: list) -> pd.DataFrame:
        return getSubstitutionsFromHost(
            matches, self.connection, self.__config.HOST
        )

    def getStartingPositions(self, matches: list) -> pd.DataFrame:
        return getStartingPositionsFromHost(
            matches, self.connection, self.__config.HOST
        )

    @staticmethod
    def generateXML(
            events: pd.DataFrame,
            lead: int,
            lag: int,
            p1Start: int,
            p2Start: int,
            p3Start: int,
            p4Start: int,
            p5Start: int
    ) -> ET.ElementTree:
        return generateXML(events, lead, lag, p1Start, p2Start, p3Start, p4Start, p5Start)