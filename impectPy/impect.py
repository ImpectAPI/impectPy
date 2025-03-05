from impectPy.config import Config
from .access_token import getAccessTokenFromUrl
from .iterations import getIterationsFromHost
from .matches import getMatchesFromHost
from .events import getEventsFromHost
from .matchsums import getPlayerMatchsumsFromHost, getSquadMatchsumsFromHost
from .iteration_averages import getPlayerIterationAveragesFromHost, getSquadIterationAveragesFromHost
from .player_scores import getPlayerMatchScoresFromHost, getPlayerIterationScoresFromHost
from .squad_scores import getSquadMatchScoresFromHost, getSquadIterationScoresFromHost
from .player_profile_scores import getPlayerProfileScoresFromHost
from .sportscode_xml import generateSportsCodeXML
from .set_pieces import getSetPiecesFromHost
from .squad_ratings import getSquadRatingsFromHost
import requests
from typing import Optional
import pandas as pd
from xml.etree import ElementTree as ET

class Impect:
    def __init__(self,config: Config = Config()):
        self.__config = config

    # Login with username and password
    def login(self, username: str, password: str) -> str:
        self.__token = getAccessTokenFromUrl(username, password, self.__config.OIDC_TOKEN_ENDPOINT)
        return self.__token

    # Use the given token for all calls of the instance
    def init(self, token: str):
        self.__token = token
    
    def getIterations(self, session: Optional[requests.Session] = None) -> pd.DataFrame:
        return getIterationsFromHost(self.__token, session, self.__config.HOST)

    def getMatches(self, iteration: int, session: Optional[requests.Session] = None) -> pd.DataFrame:
        return getMatchesFromHost(iteration, self.__token, session, self.__config.HOST)
    
    def getEvents(self, matches: list, include_kpis: bool = True, include_set_pieces: bool = True) -> pd.DataFrame:
        return getEventsFromHost(matches, self.__token, include_kpis, include_set_pieces, self.__config.HOST)
    
    def getPlayerMatchsums(self, matches: list) -> pd.DataFrame:
        return getPlayerMatchsumsFromHost(matches, self.__token, self.__config.HOST)

    def getSquadMatchsums(self, matches: list, ) -> pd.DataFrame:
        return getSquadMatchsumsFromHost(matches, self.__token, self.__config.HOST)
    
    def getPlayerIterationAverages(self, iteration: int) -> pd.DataFrame:
        return getPlayerIterationAveragesFromHost(iteration, self.__token, self.__config.HOST)
    
    def getSquadIterationAverages(self, iteration: int) -> pd.DataFrame:
        return getSquadIterationAveragesFromHost(iteration, self.__token, self.__config.HOST)

    def getPlayerMatchScores(self, matches: list, positions: list) -> pd.DataFrame:
        return getPlayerMatchScoresFromHost(matches, positions, self.__token, self.__config.HOST)

    def getPlayerIterationScores(self, iteration: int, positions: list) -> pd.DataFrame:
        return getPlayerIterationScoresFromHost(iteration, positions, self.__token, self.__config.HOST)
    
    def getSquadMatchScores(self, matches: list) -> pd.DataFrame:
        return getSquadMatchScoresFromHost(matches, self.__token, self.__config.HOST)

    def getSquadIterationScores(self, iteration: int) -> pd.DataFrame:
        return getSquadIterationScoresFromHost(iteration, self.__token, self.__config.HOST)
    
    def getPlayerProfileScores(self, iteration: int, positions: list) -> pd.DataFrame:
        return getPlayerProfileScoresFromHost(iteration, positions, self.__token, self.__config.HOST)
    
    @staticmethod
    def generateSportsCodeXML(
            events: pd.DataFrame,
            lead: int,
            lag: int,
            p1Start: int,
            p2Start: int,
            p3Start: int,
            p4Start: int,
            p5Start: int
    ) -> ET.ElementTree:
        return generateSportsCodeXML(events, lead, lag, p1Start, p2Start, p3Start, p4Start, p5Start)

    def getSetPieces(self, matches: list) -> pd.DataFrame:
        return getSetPiecesFromHost(matches, self.__token, self.__config.HOST)
    
    def getSquadRatings(self, iteration: int) -> pd.DataFrame:
        return getSquadRatingsFromHost(iteration, self.__token, self.__config.HOST)