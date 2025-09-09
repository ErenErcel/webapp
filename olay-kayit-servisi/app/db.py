import os
import os
from pathlib import Path
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Load the project root .env if present (do not override already-set shell vars)
if load_dotenv is not None:
    root_env = Path(__file__).resolve().parent.parent / ".env"
    if root_env.exists():
        load_dotenv(dotenv_path=str(root_env), override=False)  # Load environment variables from root .env file if it exists

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Prefer a single DATABASE_URL for quick local overrides
DATABASE_URL = os.getenv("DATABASE_URL")  # Get the database URL from environment variable

if not DATABASE_URL:
    DB_USER = os.getenv("POSTGRES_USER", "eventuser")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "eventpass")
    DB_NAME = os.getenv("POSTGRES_DB", "eventdb")
    DB_HOST = os.getenv("DB_HOST", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"  # Construct the database URL from individual environment variables

# Create engine with resilient defaults
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", 5)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 10)),
    future=True,
)  # Initialize the SQLAlchemy engine with connection pooling settings

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)  # Create a session factory for database interactions
Base = declarative_base()  # Declare the base class for ORM models