import pandas as pd

class BaseImpectDataframe:
    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected a pandas DataFrame, got {type(df)}")
        if "eventId" not in df.columns:
            raise ValueError("Expected 'eventId' in DataFrame columns")
        self.df = df.set_index("eventId", drop=True)

    def to_dataframe(self) -> pd.DataFrame:
        """Return a copy of the internal DataFrame."""
        return self.df.copy()

    def head(self, n: int = 5) -> pd.DataFrame:
        """Return the first n rows of the DataFrame."""
        return self.df.head(n)

    @property
    def shape(self) -> tuple:
        """Return the shape of the DataFrame."""
        return self.df.shape

    @property
    def columns(self) -> pd.Index:
        """Return the column names of the DataFrame."""
        return self.df.columns

    def __getitem__(self, key):
        return self.df[key]

    def __repr__(self):
        return repr(self.df)

    def __str__(self):
        return str(self.df)

    def _repr_html_(self):
        return self.df._repr_html_()

    def __len__(self):
        return len(self.df)

    def __iter__(self):
        return iter(self.df)

    def __contains__(self, item):
        return item in self.df

    def __eq__(self, other):
        if isinstance(other, BaseImpectDataframe):
            return self.df.equals(other.df)
        return False


class EventSequence(BaseImpectDataframe):

    @property
    def loc(self) -> "EventSequence":
        """Return the Event(s) at the given index(es)."""
        class LocIndexer:
            def __getitem__(self_inner, idx):
                if isinstance(idx, int):
                    return self.get_event(idx)
                elif isinstance(idx, slice):
                    # slice on underlying DataFrame, return wrapped EventSequence
                    sliced_df = self.df.loc[idx]
                    return EventSequence(sliced_df.reset_index())
                else:
                    raise TypeError("Index must be int or slice")

        return LocIndexer()

    @property
    def iloc(self) -> "EventSequence":
        """Return the Event(s) at the given integer location(s)."""
        class iLocIndexer:
            def __getitem__(self_inner, idx):
                if isinstance(idx, int):
                    if idx < 0 or idx >= len(self.df):
                        raise IndexError("Index out of bounds")
                    event_id = self.df.index[idx]
                    return Event(event_id, self)
                elif isinstance(idx, slice):
                    # slice on underlying DataFrame, return wrapped EventSequence
                    sliced_df = self.df.iloc[idx]
                    return EventSequence(sliced_df.reset_index())
                else:
                    raise TypeError("Index must be int or slice")

        return iLocIndexer()

    def filter_by_team(self, squadName: str) -> "EventSequence":
        """Return events for a specific team."""
        self.df = self.df[self.df["squadName"] == squadName]
        return self

    def filter_by_player(self, playerName: str) -> "EventSequence":
        """Return events for a specific player."""
        self.df = self.df[self.df['playerName'] == playerName]
        return self

    def add_lag_column(self, column: str, lag: int = 1, new_column: str = None) -> "EventSequence":
        """Add a lagged version of a column to the DataFrame."""
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")

        if new_column is None:
            new_column = f"{column}_lag{lag}"

        self.df[new_column] = self.df[column].shift(lag)

        return self

    # Add a method to EventSequence to get an Event by ID
    def get_event(self, eventId: int) -> "Event":
        return Event(eventId, self)


class Event:
    def __init__(self, eventId: int, sequence: "EventSequence"):
        self.event_id = eventId
        self.sequence = sequence

        if eventId not in self.sequence.df.index:
            raise ValueError(f"eventId {eventId} not found in EventSequence")

        self.row = self.sequence.df.loc[eventId]

    def __repr__(self):
        return repr(self.row)

    def _repr_html_(self):
        return self.row.to_frame()._repr_html_()

    @property
    def shape(self):
        """Pretend to be a 1-row DataFrame (n_rows, n_columns)."""
        return (1, len(self.row))

    def head(self, n: int = 5) -> pd.DataFrame:
        """Return the Event as a one-row DataFrame."""
        return self.row.to_frame().T

    def __getattr__(self, attr):
        """Delegate attribute access to the row Series."""
        if attr in self.row:
            return self.row[attr]
        raise AttributeError(f"'Event' object has no attribute '{attr}'")

    def get_next_event(self) -> "Event":
        """Return the next Event in the same match, ordered by gameTimeInSec."""
        current_match_id = self.row["matchId"]
        current_time = self.row["gameTimeInSec"]

        df = self.sequence.df
        candidates = df[(df["matchId"] == current_match_id) & (df["gameTimeInSec"] > current_time)]
        if candidates.empty:
            return NullEvent()
        next_event_id = candidates.sort_values("gameTimeInSec").index[0]
        return Event(next_event_id, self.sequence)

    def get_previous_event(self) -> "Event":
        """Return the previous Event in the same match, ordered by gameTimeInSec."""
        current_match_id = self.row["matchId"]
        current_time = self.row["gameTimeInSec"]

        df = self.sequence.df
        candidates = df[(df["matchId"] == current_match_id) & (df["gameTimeInSec"] < current_time)]
        if candidates.empty:
            return NullEvent()
        previous_event_id = candidates.sort_values("gameTimeInSec", ascending=False).index[0]
        return Event(previous_event_id, self.sequence)

    def to_series(self) -> pd.Series:
        """Return the raw event data as a pandas Series."""
        return self.row


# noinspection PyMissingConstructor
class NullEvent(Event):
    def __init__(self):
        self.event_id = None
        self.row = pd.Series(dtype=object)
        self.sequence = None

    def __getattr__(self, attr):
        return None

    def __repr__(self):
        return "<NullEvent>"

    def _repr_html_(self):
        return "<NullEvent>"