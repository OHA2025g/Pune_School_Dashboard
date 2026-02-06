"""Authentication utilities"""
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import secrets

# Configuration
#
# IMPORTANT (local/dev):
# If JWT_SECRET_KEY is not set, we must NOT generate a random key on every boot,
# otherwise any server restart invalidates existing browser tokens and breaks
# auth-protected endpoints (e.g. Advanced Analytics predictions).
#
# For production, ALWAYS set JWT_SECRET_KEY via environment.
SECRET_KEY = (
    os.environ.get("JWT_SECRET_KEY")
    or os.environ.get("SECRET_KEY")
    or "mahaedume-dev-jwt-secret-change-me"
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Security scheme
security = HTTPBearer(auto_error=False)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hash a password"""
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def create_reset_token(email: str) -> str:
    """Create a password reset token"""
    expire = datetime.now(timezone.utc) + timedelta(hours=1)
    to_encode = {"sub": email, "exp": expire, "type": "reset"}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_reset_token(token: str) -> Optional[str]:
    """Verify a password reset token and return email"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "reset":
            return None
        return payload.get("sub")
    except JWTError:
        return None

def decode_token(token: str) -> Optional[dict]:
    """Decode a JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None

async def get_current_user_optional(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user from token (optional - returns None if no token)"""
    if credentials is None:
        return None
    
    token = credentials.credentials
    payload = decode_token(token)
    if payload is None:
        return None
    
    return {
        "email": payload.get("sub"),
        "role": payload.get("role"),
        "district_code": payload.get("district_code"),
        "user_id": payload.get("user_id"),
        "full_name": payload.get("full_name")
    }

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user from token (required)"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    if credentials is None:
        raise credentials_exception
    
    token = credentials.credentials
    payload = decode_token(token)
    if payload is None:
        raise credentials_exception
    
    return {
        "email": payload.get("sub"),
        "role": payload.get("role"),
        "district_code": payload.get("district_code"),
        "user_id": payload.get("user_id"),
        "full_name": payload.get("full_name")
    }

def require_role(*allowed_roles):
    """Decorator to require specific roles"""
    async def role_checker(current_user: dict = Depends(get_current_user)):
        if current_user["role"] not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required roles: {', '.join(allowed_roles)}"
            )
        return current_user
    return role_checker

def require_admin(current_user: dict = Depends(get_current_user)):
    """Require admin role"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user

def require_export_permission(current_user: dict = Depends(get_current_user)):
    """Require export permission"""
    if current_user["role"] == "viewer":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Export permission denied for viewer role"
        )
    return current_user
