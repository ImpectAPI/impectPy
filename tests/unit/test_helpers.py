import warnings
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from impectPy.helpers import resolve_matches, MatchResolution, RateLimitedAPI


def _match_df(match_id: int, iteration_id: int, last_calc=None):
    """Return a minimal match-info DataFrame as the API would produce it."""
    return pd.DataFrame([{
        "id": match_id,
        "iterationId": iteration_id,
        "lastCalculationDate": last_calc,
    }])


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

class TestInputValidation:
    def test_raises_if_matches_not_list(self):
        connection = MagicMock(spec=RateLimitedAPI)
        with pytest.raises(Exception, match="Argument 'matches' must be a list of integers."):
            resolve_matches(123, connection, "https://api.example.com")

    def test_raises_if_matches_is_string(self):
        connection = MagicMock(spec=RateLimitedAPI)
        with pytest.raises(Exception, match="Argument 'matches' must be a list of integers."):
            resolve_matches("101,102", connection, "https://api.example.com")


# ---------------------------------------------------------------------------
# All matches unavailable / empty
# ---------------------------------------------------------------------------

class TestAllMatchesEmpty:
    def test_raises_if_all_responses_are_empty_dataframes(self):
        connection = MagicMock(spec=RateLimitedAPI)
        with patch("impectPy.helpers.safe_execute", return_value=pd.DataFrame()):
            with pytest.raises(Exception, match="All supplied matches are unavailable or forbidden"):
                resolve_matches([1, 2], connection, "https://api.example.com")

    def test_raises_if_all_matches_are_unavailable(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df1 = _match_df(match_id=1, iteration_id=5, last_calc=None)
        df2 = _match_df(match_id=2, iteration_id=5, last_calc=None)
        with patch("impectPy.helpers.safe_execute", side_effect=[df1, df2]):
            with pytest.raises(Exception, match="All supplied matches are unavailable or forbidden"):
                resolve_matches([1, 2], connection, "https://api.example.com")


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestHappyPath:
    def test_returns_match_resolution_namedtuple(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df1 = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")
        df2 = _match_df(match_id=102, iteration_id=5, last_calc="2024-01-01")
        with patch("impectPy.helpers.safe_execute", side_effect=[df1, df2]):
            result = resolve_matches([101, 102], connection, "https://api.example.com")
        assert isinstance(result, MatchResolution)

    def test_match_data_contains_all_valid_rows(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df1 = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")
        df2 = _match_df(match_id=102, iteration_id=5, last_calc="2024-01-01")
        with patch("impectPy.helpers.safe_execute", side_effect=[df1, df2]):
            result = resolve_matches([101, 102], connection, "https://api.example.com")
        assert len(result.match_data) == 2

    def test_matches_list_contains_valid_ids(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df1 = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")
        df2 = _match_df(match_id=102, iteration_id=5, last_calc="2024-01-01")
        with patch("impectPy.helpers.safe_execute", side_effect=[df1, df2]):
            result = resolve_matches([101, 102], connection, "https://api.example.com")
        assert sorted(result.matches) == [101, 102]

    def test_iterations_deduplicated(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df1 = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")
        df2 = _match_df(match_id=102, iteration_id=5, last_calc="2024-01-01")
        with patch("impectPy.helpers.safe_execute", side_effect=[df1, df2]):
            result = resolve_matches([101, 102], connection, "https://api.example.com")
        assert result.iterations == [5]


# ---------------------------------------------------------------------------
# Unavailable matches
# ---------------------------------------------------------------------------

class TestUnavailableMatches:
    def test_excludes_unavailable_match_from_matches_list(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df_ok = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")
        df_na = _match_df(match_id=102, iteration_id=5, last_calc=None)
        with patch("impectPy.helpers.safe_execute", side_effect=[df_ok, df_na]):
            result = resolve_matches([101, 102], connection, "https://api.example.com")
        assert result.matches == [101]
        assert 102 not in result.matches

    def test_emits_warning_for_unavailable_match(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df_ok = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")
        df_na = _match_df(match_id=102, iteration_id=5, last_calc=None)
        with patch("impectPy.helpers.safe_execute", side_effect=[df_ok, df_na]):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                resolve_matches([101, 102], connection, "https://api.example.com")
        assert any("not available yet" in str(w.message) for w in caught)

    def test_unavailable_match_still_in_match_data(self):
        """match_data includes unavailable rows — callers may need them for merges."""
        connection = MagicMock(spec=RateLimitedAPI)
        df_ok = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")
        df_na = _match_df(match_id=102, iteration_id=5, last_calc=None)
        with patch("impectPy.helpers.safe_execute", side_effect=[df_ok, df_na]):
            result = resolve_matches([101, 102], connection, "https://api.example.com")
        assert len(result.match_data) == 2


# ---------------------------------------------------------------------------
# Forbidden matches
# ---------------------------------------------------------------------------

class TestForbiddenMatches:
    def test_forbidden_match_excluded_and_warns(self):
        connection = MagicMock(spec=RateLimitedAPI)
        df_ok = _match_df(match_id=101, iteration_id=5, last_calc="2024-01-01")

        def fake_safe_execute(func, *args, identifier, forbidden_list, **kwargs):
            if identifier == 102:
                forbidden_list.append(102)
                return pd.DataFrame()
            return df_ok

        with patch("impectPy.helpers.safe_execute", side_effect=fake_safe_execute):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = resolve_matches([101, 102], connection, "https://api.example.com")

        assert result.matches == [101]
        assert any("forbidden" in str(w.message) for w in caught)
