"""Publish an immutable policy catalog after applying the SQL migration."""
import argparse

from main import db
from policy_safety import STAGING, publish_catalog


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog")
    args = parser.parse_args()
    with db() as conn:
        publish_catalog(conn, args.catalog, STAGING)
    print("Policy catalog saved")
