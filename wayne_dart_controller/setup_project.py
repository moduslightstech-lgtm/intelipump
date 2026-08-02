import os

print("Creating directory structure...")
os.makedirs("config", exist_ok=True)
os.makedirs("src/driver", exist_ok=True)
os.makedirs("src/protocol", exist_ok=True)
os.makedirs("src/core", exist_ok=True)
os.makedirs("tests", exist_ok=True)
print("Folders ready!")