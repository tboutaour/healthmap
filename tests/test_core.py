from datetime import date, datetime

import duckdb
import httpx

from healthmap.dates import daterange, is_recent, windows
from healthmap.garmin.auth import _extract_token
from healthmap.load.build_duckdb import build, strava_stream_rows
from healthmap.load.flatten import flatten, semicircles_to_degrees
from healthmap.raw_store import RawStore
from healthmap.strava.client import parse_pair, seconds_to_next_quarter, usage_state


def test_windows_cover_range_without_overlap():
    w = list(windows(date(2026, 1, 1), date(2026, 3, 1), 28))
    assert w[0] == (date(2026, 1, 1), date(2026, 1, 28))
    assert w[-1][1] == date(2026, 3, 1)
    for (_, e1), (s2, _) in zip(w, w[1:], strict=False):
        assert (s2 - e1).days == 1
    assert all((e - s).days < 28 for s, e in w)


def test_daterange_step_and_recent():
    assert len(list(daterange(date(2026, 1, 1), date(2026, 1, 31), 7))) == 5
    assert is_recent(date(2026, 11, 4), date(2026, 11, 6), 3)
    assert not is_recent(date(2026, 11, 1), date(2026, 11, 6), 3)


def test_flatten_nested_and_drops_lists():
    row = flatten({"a": 1, "b": {"c": 2, "d": {"e": 3}}, "l": [1, 2]})
    assert row == {"a": 1, "b_c": 2, "b_d_e": 3}
    assert flatten({"b": {"c": {"d": 1}}}, max_depth=2) == {}


def test_semicircles():
    assert abs(semicircles_to_degrees(2**30) - 90.0) < 1e-9
    assert semicircles_to_degrees(None) is None


def test_raw_store_idempotent_and_empty(tmp_path):
    s = RawStore(tmp_path)
    assert not s.has("g", "ep", "k")
    s.write_json("g", "ep", "k", {"x": 1})
    assert s.has("g", "ep", "k") and s.read_json("g", "ep", "k") == {"x": 1}
    s.mark_empty("g", "ep", "k2", "404")
    assert s.has("g", "ep", "k2")
    s.write_json("g", "ep", "k2", [1])  # al llegar datos se elimina la marca de vacío
    assert not s.path("g", "ep", "k2", "empty").exists()


def test_garmin_token_variants():
    assert _extract_token({"access_token": "abc", "expires_in": 100}) == ("abc", 100)
    assert _extract_token({"accessToken": "x"})[0] == "x"
    assert _extract_token("tok ")[0] == "tok"


def test_strava_rate_limit_parsing():
    assert parse_pair("200,2000") == (200, 2000)
    assert parse_pair(None) is None
    h = httpx.Headers({"X-ReadRateLimit-Limit": "100,1000", "X-ReadRateLimit-Usage": "98,10"})
    assert usage_state(h) == (True, False)
    h = httpx.Headers({"X-RateLimit-Limit": "200,2000", "X-RateLimit-Usage": "5,1999"})
    assert usage_state(h) == (False, True)
    assert 0 < seconds_to_next_quarter(datetime(2026, 1, 1, 10, 14, 30)) <= 40


def test_strava_streams_long_format():
    payload = {"time": {"data": [0, 1]}, "heartrate": {"data": [120, 121]},
               "latlng": {"data": [[39.5, 2.6], [39.6, 2.7]]}}
    rows = list(strava_stream_rows("42", payload))
    assert rows[1][:5] == ["42", 1, 1, None, 39.6] and rows[1][7] == 121


def test_build_end_to_end(tmp_path, monkeypatch):
    from healthmap import config

    monkeypatch.setattr(config.settings, "start_date", date(2026, 1, 1))
    s = RawStore(tmp_path / "raw")
    s.write_json("garmin", "daily_summary", "2026-01-02",
                 {"calendarDate": "2026-01-02", "restingHeartRate": 45, "totalSteps": 9000})
    s.write_json("garmin", "sleep", "2026-01-01_2026-01-31", [
        {"calendarDate": "2026-01-02", "sleepTimeSeconds": 27000,
         "sleepScores": {"overall": {"value": 82}}}])
    s.write_json("garmin", "hrv_daily", "2026-01-01_2026-01-31",
                 {"hrvSummaries": [{"calendarDate": "2026-01-02", "lastNightAvg": 60}]})
    s.write_json("garmin", "heart_rate", "2026-01-02", {
        "calendarDate": "2026-01-02",
        "heartRateValueDescriptors": [{"index": 0, "key": "timestamp"},
                                      {"index": 1, "key": "heartrate"}],
        "heartRateValues": [[1767312000000, 50], [1767312120000, 52]]})
    s.write_json("strava", "activities_list", "x_001", [
        {"id": 1, "start_date": "2026-01-02T07:00:00Z", "start_date_local": "2026-01-02T08:00:00Z",
         "moving_time": 3600, "distance": 12000.0}])
    s.write_json("strava", "activity_detail", "1", {
        "id": 1, "laps": [{"lap_index": 1, "distance": 6000}, {"lap_index": 2, "distance": 6000}]})

    db = tmp_path / "h.duckdb"
    counts = build(store=s, db_path=db, staging=tmp_path / "staging")
    assert counts["garmin_daily_summary"] == 1 and counts["strava_laps"] == 2
    assert counts["garmin_hr_intraday"] == 2

    con = duckdb.connect(str(db))
    row = con.execute("""SELECT resting_hr, sleep_hours, sleep_score, hrv_last_night,
                                strava_activities, strava_km
                         FROM v_daily_state WHERE day = DATE '2026-01-02'""").fetchone()
    assert row == (45, 7.5, 82, 60, 1, 12.0)
