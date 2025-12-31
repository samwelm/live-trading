# constants/defs.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import values from .env
API_KEY = os.environ.get("API_KEY")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID")
OANDA_URL = os.environ.get("OANDA_URL")
MONGO_CONN_STR = os.environ.get("MONGO_CONN_STR")

# Secure header for streaming API
SECURE_HEADER = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# Constants for trading operations
SELL = -1
BUY = 1
NONE = 0


# Time granularity constants (in seconds)
GRANULARITY = {
    # Seconds
    "S5": 5,             # 5 seconds
    "S10": 10,           # 10 seconds
    "S15": 15,           # 15 seconds
    "S30": 30,           # 30 seconds
    
    # Minutes
    "M1": 60,            # 1 minute
    "M2": 120,           # 2 minutes
    "M3": 180,           # 3 minutes
    "M5": 300,           # 5 minutes
    "M10": 600,          # 10 minutes
    "M15": 900,          # 15 minutes
    "M30": 1800,         # 30 minutes
    
    # Hours
    "H1": 3600,          # 1 hour
    "H2": 7200,          # 2 hours
    "H3": 10800,         # 3 hours
    "H4": 14400,         # 4 hours
    "H6": 21600,         # 6 hours
    "H8": 28800,         # 8 hours
    "H12": 43200,        # 12 hours
    
    # Days
    "D": 86400,          # 1 day
    "D2": 172800,        # 2 days
    "D3": 259200,        # 3 days
    
    # Weeks
    "W": 604800,         # 1 week
    "W2": 1209600,       # 2 weeks
    
    # Months (approximate)
    "MN": 2592000,       # 30 days
}