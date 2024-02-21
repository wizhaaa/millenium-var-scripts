import os
from dotenv import load_dotenv
load_dotenv()

host_val = os.getenv("HOST")
database_val = os.getenv("DATABASE")
user_val = os.getenv("USER")
password_val = os.getenv("PASSWORD")
port_val = os.getenv("PORT")

SECURITY_WRITE_ENABLE = True
POSITIONS_WRITE_ENABLE = False
PNL_WRITE_ENABLE = False

HOST = host_val
DATABASE = database_val
USER = user_val
PASSWORD = password_val
PORT = port_val
