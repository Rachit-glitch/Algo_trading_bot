from ib_insync import *
from dotenv import load_dotenv
import os

load_dotenv()

ib = IB()
ib.connect(
    os.getenv("IB_HOST", "127.0.0.1"),
    int(os.getenv("IB_PORT", "7497")),
    clientId=int(os.getenv("IB_CLIENT_ID", "17"))
)
print("Connected:", ib.isConnected())
ib.disconnect()
