"""User model and authentication schemas"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from enum import Enum
from datetime import datetime

class UserRole(str, Enum):
    ADMIN = "admin"
    STATE_OFFICER = "state_officer"
    DISTRICT_OFFICER = "district_officer"
    VIEWER = "viewer"

class UserBase(BaseModel):
    email: EmailStr
    full_name: str
    role: UserRole = UserRole.VIEWER
    district_code: Optional[str] = None  # For district officers
    is_active: bool = True

class UserCreate(UserBase):
    password: str = Field(..., min_length=6)

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    role: Optional[UserRole] = None
    district_code: Optional[str] = None
    is_active: Optional[bool] = None

class UserInDB(UserBase):
    id: str
    hashed_password: str
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None

class UserResponse(UserBase):
    id: str
    created_at: datetime

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict

class TokenData(BaseModel):
    email: Optional[str] = None
    role: Optional[str] = None

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class PasswordResetRequest(BaseModel):
    email: EmailStr

class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str = Field(..., min_length=6)

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=6)

# Role permissions
ROLE_PERMISSIONS = {
    UserRole.ADMIN: {
        "can_view_all": True,
        "can_export": True,
        "can_manage_users": True,
        "can_import_data": True,
        "districts": "all"
    },
    UserRole.STATE_OFFICER: {
        "can_view_all": True,
        "can_export": True,
        "can_manage_users": False,
        "can_import_data": False,
        "districts": "all"
    },
    UserRole.DISTRICT_OFFICER: {
        "can_view_all": False,
        "can_export": True,
        "can_manage_users": False,
        "can_import_data": False,
        "districts": "assigned"
    },
    UserRole.VIEWER: {
        "can_view_all": True,
        "can_export": False,
        "can_manage_users": False,
        "can_import_data": False,
        "districts": "all"
    }
}
