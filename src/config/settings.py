from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CHUNK_SIZE = int(os.environ["CHUNK_SIZE"])
DATA_DIR = os.environ["DATA_DIR"]
POSTGRES_CONN_ID = os.environ["POSTGRES_CONN_ID"]
