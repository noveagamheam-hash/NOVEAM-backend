import firebase_admin
from firebase_admin import credentials

if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

print("Firebase Admin initialized successfully")
