import os


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-local-caja")
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL", "sqlite:///instance/caja.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
