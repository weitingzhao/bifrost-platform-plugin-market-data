"""Plugin analytics package.

Daily upserts (max-pain / ATM IV / PCR / IV percentile) live in
``bifrost_research``. Live max-pain compute used to sit here
(``max_pain_math``); it had no caller and is gone (TD-102). Persisted reads
stay on ``api/analytics.py``.
"""
