# define version attribute
__version__ = "2.5.3"

# import modules
from .access_token import getAccessToken
from .iterations import getIterations
from .matches import getMatches
from .events import getEvents
from .player_matchsums import getPlayerMatchsums
from .squad_matchsums import getSquadMatchsums
from .player_iteration_averages import getPlayerIterationAverages
from .squad_iteration_averages import getSquadIterationAverages
from .player_match_scores import getPlayerMatchScores
from .player_iteration_scores import getPlayerIterationScores
from .squad_match_scores import getSquadMatchScores
from .squad_iteration_scores import getSquadIterationScores
from .player_profile_scores import getPlayerProfileScores
from .generate_xml import generateXML
from .set_pieces import getSetPieces
from .squad_ratings import getSquadRatings
from .squad_coefficients import getSquadCoefficients
from .formations import getFormations
from .substitutions import getSubstitutions
from .starting_positions import getStartingPositions
from .config import Config as Config
from .impect import Impect as Impect