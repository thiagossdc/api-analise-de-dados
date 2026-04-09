from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional

class UserCreate(BaseModel):
    username: str
    email: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class UserOut(BaseModel):
    id: int
    username: str
    email: str
    created_at: datetime

    class Config:
        from_attributes = True

class DataUploadCreate(BaseModel):
    filename: str
    file_type: str
    data_preview: str

class DataUploadOut(BaseModel):
    id: int
    filename: str
    file_type: str
    upload_date: datetime
    data_preview: str

    class Config:
        from_attributes = True
