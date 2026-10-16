import firebase_admin
from firebase_admin import credentials

def initialize_firebase():
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)

    return firebase_admin.get_app()
