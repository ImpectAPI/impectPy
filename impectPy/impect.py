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
        return generateXML(
            events, lead, lag, p1Start, p2Start, p3Start, p4Start, p5Start, codeTag, squad,
            perspective, labels, kpis, labelSorting, sequencing, buckets
        )