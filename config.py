import os
import secrets

ENVIRON = os.environ.get("ENVIRON", "PRODUCTION")
DEBUG = True if os.environ.get("DEBUG", "True") == "True" else False
IN_DOCKER = True if os.environ.get("IN_DOCKER", False) == "True" else False

BASE_PATH = os.environ.get("AI_RESPONSE_GENIE_EVALUATION", "/home/user/Projects/ai_response_genie_evaluation")
BASE_DATA_PATH = os.environ.get("AI_RESPONSE_GENIE_EVALUATION_DATA",
                                "/home/user/Projects/ai_response_genie_evaluation/ai_response_genie_evaluation_data")


SESSION_KEY = secrets.token_hex(8)

print(f"connecting to the {ENVIRON} database")
print(f"CurrentSessionKey: {SESSION_KEY}")
print(f"DEBUG: {DEBUG}")
print(f"IN_DOCKER: {IN_DOCKER}")


HOST = "0.0.0.0"
APP_MAIN_PORT = int(os.environ.get("APP_MAIN_PORT", 4434))

DEFAULT_LANGUAGE = "en"
# telegram credentials
BOT_ID = "6344429111:AAEoQOqs3O3egRQvfGwYYiTn7M3LHrXXmgQ"
CHAT_ID = "-4084960830"

DAILY_GENIE_EVALUATION_G_CHAT_KEY = "https://chat.googleapis.com/v1/spaces/AAAASDu4ezk/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=u0W-3vgb9FmVtlOjqA0oDizPmmiQjyFARhO2Uz3n8_0"

# CONSTANT HEADER
HEADER = "application/json"

# DATABASE CONNECTION CREDENTIAL
# host and port declared here wont be used before the project uses DSN for connection
# server, port and db are specified in odbcinst.ini

if ENVIRON == "LOCAL":
    MSSQL_AUTH = {"r": {
        "username": "appuser",
        "password": r"Locobuzz@123",
        "db_name": "Spatialrss"
    },
        "rw": {
            "username": "appuser",
            "password": r"Locobuzz@123",
            "db_name": "Spatialrss"
        }
    }
elif ENVIRON in ["DEVELOPMENT", "PRODUCTION"]:
    MSSQL_AUTH = {"r": {
        "username": "RDUBSU6KMWWn3DQZNNzG_AI_Application",
        "password": r">\Wt<C}H&[$222TEp`5krrZ```",
        "db_name": "Spatialrss"
    },
        "rw": {
            "username": "RDUBSU6KMWWn3DQZNNzG_AI_Application",
            "password": r">\Wt<C}H&[$222TEp`5krrZ```",
            "db_name": "Spatialrss"
        }}
else:
    raise ValueError("Unknown ENVIRON")
