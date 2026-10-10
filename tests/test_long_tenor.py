from datetime import date, timedelta

from bifrost_market_data.long_tenor import judge_breadth


def test_missing_day_is_zero_and_not_removed_from_trailing_window():
    days = [date(2026, 8, 10)+timedelta(days=i) for i in range(5)]
    counts = [(d, 100) for d in days[:3]] + [(days[4], 79)]
    out = judge_breadth(counts, days)
    assert out[3]['status'] == 'missing_data' and out[3]['symbols'] == 0
    assert out[4]['status'] == 'narrow'
    assert out[0]['status'] == 'history_insufficient'


def test_read_failure_never_becomes_empty_or_clean():
    assert judge_breadth(None, []) == [{'status': 'unprobed', 'reason': 'breadth query failed'}]


def test_zero_baseline_has_no_claim_of_good_coverage():
    days = [date(2026, 8, 10)+timedelta(days=i) for i in range(5)]
    assert judge_breadth([], days)[-1]['status'] == 'history_insufficient'
