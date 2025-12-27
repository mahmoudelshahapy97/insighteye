# app/schemas/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_username: str
    smtp_password: str
    otp_sender_email: str
    
    class Config:
        env_file = ".env"