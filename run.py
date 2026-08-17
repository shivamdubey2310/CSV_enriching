"""
CLI entry point for the B2B Data Enrichment Pipeline.
"""

import argparse
import asyncio
import os
import sys

# Ensure src is in the system path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.pipeline import run_pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified B2B Enrichment Pipeline CLI")
    parser.add_argument(
        "-i",
        "--input",
        default="data/raw/*.csv",
        help="Input CSV file path or wildcard (default: 'data/raw/*.csv')",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="data/processed/master_enriched_final.csv",
        help="Target output CSV path (default: 'data/processed/master_enriched_final.csv')",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=15,
        help="Concurrency batch size (default: 15)",
    )

    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs(".cache", exist_ok=True)

    asyncio.run(run_pipeline(args.input, args.output, args.batch_size))