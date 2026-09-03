#!/usr/bin/env python3
"""Verify a CSV of leads against MillionVerifier and split it by deliverability.

    export MILLIONVERIFIER_API_KEY=...
    python verify_emails.py leads.csv --out-dir out
"""

import sys

from mvverify.cli import main

if __name__ == "__main__":
    sys.exit(main())
