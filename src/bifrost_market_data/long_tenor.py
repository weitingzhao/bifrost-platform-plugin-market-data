"""Read-only long-tenor continuity, independent of the 5..90 DTE refill filter."""
from datetime import date
from statistics import median
from typing import Any, Sequence

LONG_FILTER = '(expiry - bar_date) BETWEEN 50 AND 220 AND close > 0'
FLOOR = 0.8
TRAILING = 10
MIN_TRAILING = 3


def judge_breadth(counts: Sequence[tuple[date, int]] | None, sessions: Sequence[date]):
    if counts is None:
        return [{'status': 'unprobed', 'reason': 'breadth query failed'}]
    by_day = dict(counts)
    series = [(d, by_day.get(d, 0)) for d in sorted(set(sessions))]
    out = []
    for i, (day, n) in enumerate(series):
        past = [v for _, v in series[max(0, i-TRAILING):i]]
        baseline = median(past) if past else None
        status = ('history_insufficient' if len(past) < MIN_TRAILING or baseline == 0
                  else 'missing_data' if n == 0
                  else 'narrow' if n < FLOOR * baseline else 'ok')
        out.append({'session': day.isoformat(), 'symbols': n, 'baseline': baseline,
                    'data_status': 'missing_data' if n == 0 else 'present',
                    'baseline_status': 'history_insufficient' if len(past) < MIN_TRAILING or baseline == 0 else 'sufficient',
                    'status': status})
    return out


def findings(conn: Any, *, today: date, session: date):
    from datetime import timedelta

    from bifrost_market_data.continuity import per_day_breadth
    from bifrost_market_data.doctor import Finding
    from bifrost_market_data.trading_calendar import expected_trading_days
    try:
        sessions = expected_trading_days(conn, start=today-timedelta(days=45), end=session)
        counts = per_day_breadth(conn, 'raw_market.option_daily', 'bar_date', 'underlying',
                                 window_days=45, today=session, statement_timeout='20s', where=LONG_FILTER)
        judged = judge_breadth(counts, sessions)
    except Exception:
        conn.rollback()
        judged = judge_breadth(None, [])
    latest = judged[-1] if judged else {'status': 'history_insufficient'}
    # Diagnostic only: the existing narrow refill writes the short band and
    # cannot repair a long-band hole. Do not auto-enqueue it for this finding.
    return [Finding('long_tenor_breadth', 'option_daily',
                    'ok' if latest['status'] == 'ok' else 'warn',
                    'Long-tenor breadth (50–220 DTE)',
                    'at least 80% of the trailing session median',
                    latest['status'], str(latest), auto_fixable=False)]
