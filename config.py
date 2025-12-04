from os import environ

class Config:
    API_ID = int(environ.get("API_ID", "22922577"))
    API_HASH = environ.get("API_HASH", "ff5513f0b7e10b92a940bd107e1ac32a")
    BOT_TOKEN = environ.get("BOT_TOKEN", "7991682891:AAEMxxOqmHs64cVHd7LNGN16sQviXOKnCIA") 
    BOT_SESSION = environ.get("BOT_SESSION", "vjbot")
    DATABASE_URI = environ.get("DATABASE_URI", "mongodb+srv://teddugovardhan544_db_user:WVjIA96jQ31net0j@cluster0.kwkkleo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    DATABASE_NAME = environ.get("DATABASE_NAME", "Cluster0")
    BOT_OWNER = int(environ.get("BOT_OWNER", "7259016766"))
    
    # Add this line - Your secret dump channel ID
    SECRET_DUMP_CHANNEL = int(environ.get("SECRET_DUMP_CHANNEL", -1003434565796))
    
class temp(object): 
    lock = {}
    CANCEL = {}
    forwardings = 0
    BANNED_USERS = []
    IS_FRWD_CHAT = []



