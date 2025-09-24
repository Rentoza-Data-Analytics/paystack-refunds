#!/usr/bin/env python3
"""
Connects to Paystack SFTP, finds refund CSV files, and downloads them locally.
"""

import os
import sys
import fnmatch
import logging
import posixpath  # <-- FIX: use this for SFTP remote paths
from pathlib import Path
from dotenv import load_dotenv
import paramiko
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Load .env from repo root if present
load_dotenv()

# Config from env
SFTP_HOST = os.getenv("SFTP_HOST", "rentoza-sftp.production.paystack.co")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER", "sftp_rentoza")
SFTP_PASS = os.getenv("SFTP_PASS")  # set in .env or environment
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR", "/refunds")
SFTP_REMOTE_PATTERN = os.getenv("SFTP_REMOTE_PATTERN", "*.csv")

# Local folders
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

if not SFTP_PASS:
    logging.warning("SFTP_PASS not set. Please set it in .env or environment variables.")


def connect_sftp(host, port, username, password, timeout=30):
    """Return an open SFTPClient"""
    logging.info(f"Connecting to SFTP {username}@{host}:{port}")
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    logging.info("SFTP connected")
    return sftp, transport


def list_remote_files(sftp, remote_dir, pattern):
    """List files in remote_dir matching pattern (fnmatch)"""
    files = []
    try:
        names = sftp.listdir(remote_dir)
    except IOError as e:
        logging.error(f"Failed to list directory {remote_dir}: {e}")
        raise
    for name in names:
        if fnmatch.fnmatch(name, pattern):
            files.append(name)
    logging.info(f"Found {len(files)} files matching pattern '{pattern}' in {remote_dir}")
    return files


def download_file(sftp, remote_dir, filename, local_dir: Path):
    """Download a single file"""
    remote_path = posixpath.join(remote_dir, filename)  # FIXED: use posixpath for SFTP
    local_path = local_dir / filename

    if local_path.exists():
        logging.info(f"Skipping {filename}, already exists at {local_path}")
        return local_path

    logging.info(f"Downloading {remote_path} -> {local_path}")
    sftp.get(remote_path, str(local_path))
    logging.info(f"Downloaded {filename}")
    return local_path


def main():
    # Connect to SFTP
    try:
        sftp, transport = connect_sftp(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASS)
    except Exception as e:
        logging.error("Failed to connect to SFTP: %s", e)
        sys.exit(1)

    try:
        files = list_remote_files(sftp, SFTP_REMOTE_DIR, SFTP_REMOTE_PATTERN)
        if not files:
            logging.info("No matching files found. Exiting.")
            return

        for fname in files:
            try:
                local_file = download_file(sftp, SFTP_REMOTE_DIR, fname, DOWNLOAD_DIR)

                # Optional: validate/clean CSV with pandas
                try:
                    df = pd.read_csv(local_file)
                    logging.info(f"Validated CSV: {local_file}")
                except Exception as e:
                    logging.warning(f"Could not parse CSV with pandas ({local_file}): {e}")

            except Exception as e:
                logging.error(f"Failed to download {fname}: {e}")
                continue

    finally:
        try:
            sftp.close()
            transport.close()
            logging.info("Closed SFTP connection")
        except Exception:
            pass


if __name__ == "__main__":
    main()