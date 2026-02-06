"""Authentication routes"""
from fastapi import APIRouter, HTTPException, status, Depends
from datetime import datetime, timezone
from typing import Optional
import uuid

from models.user import (
    UserCreate, UserUpdate, UserResponse, UserRole,
    LoginRequest, Token, PasswordResetRequest, PasswordResetConfirm,
    ChangePasswordRequest, ROLE_PERMISSIONS
)
from utils.auth import (
    verify_password, get_password_hash, create_access_token,
    create_reset_token, verify_reset_token, get_current_user, require_admin
)

router = APIRouter(prefix="/auth", tags=["Authentication"])

# Database will be injected
db = None

def init_db(database):
    global db
    db = database

@router.post("/login", response_model=Token)
async def login(request: LoginRequest):
    """Login with email and password"""
    user = await db.users.find_one({"email": request.email})
    
    if not user or not verify_password(request.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )
    
    if not user.get("is_active", True):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is disabled"
        )
    
    # Create token
    token_data = {
        "sub": user["email"],
        "role": user["role"],
        "user_id": user["id"],
        "full_name": user["full_name"],
        "district_code": user.get("district_code")
    }
    access_token = create_access_token(token_data)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "email": user["email"],
            "full_name": user["full_name"],
            "role": user["role"],
            "district_code": user.get("district_code"),
            "permissions": ROLE_PERMISSIONS.get(UserRole(user["role"]), {})
        }
    }

@router.post("/google-login", response_model=Token)
async def google_login(google_token: dict):
    """Login with Google OAuth"""
    # Verify Google token (simplified - in production use Google's API)
    email = google_token.get("email")
    name = google_token.get("name", "")
    
    if not email:
        raise HTTPException(status_code=400, detail="Invalid Google token")
    
    # Find or create user
    user = await db.users.find_one({"email": email})
    
    if not user:
        # Create new user with viewer role
        user_id = str(uuid.uuid4())
        user = {
            "id": user_id,
            "email": email,
            "full_name": name,
            "role": UserRole.VIEWER.value,
            "is_active": True,
            "hashed_password": "",  # No password for Google users
            "auth_provider": "google",
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        await db.users.insert_one(user)
    
    if not user.get("is_active", True):
        raise HTTPException(status_code=403, detail="Account is disabled")
    
    token_data = {
        "sub": user["email"],
        "role": user["role"],
        "user_id": user["id"],
        "full_name": user["full_name"],
        "district_code": user.get("district_code")
    }
    access_token = create_access_token(token_data)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "email": user["email"],
            "full_name": user["full_name"],
            "role": user["role"],
            "permissions": ROLE_PERMISSIONS.get(UserRole(user["role"]), {})
        }
    }

@router.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """Get current user information"""
    user = await db.users.find_one({"email": current_user["email"]})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {
        "id": user["id"],
        "email": user["email"],
        "full_name": user["full_name"],
        "role": user["role"],
        "district_code": user.get("district_code"),
        "is_active": user.get("is_active", True),
        "created_at": user.get("created_at"),
        "permissions": ROLE_PERMISSIONS.get(UserRole(user["role"]), {})
    }

@router.post("/password-reset-request")
async def request_password_reset(request: PasswordResetRequest):
    """Request a password reset"""
    user = await db.users.find_one({"email": request.email})
    
    # Always return success to prevent email enumeration
    if user:
        reset_token = create_reset_token(request.email)
        # In production, send email with reset link
        # For now, store token in DB
        await db.password_resets.insert_one({
            "email": request.email,
            "token": reset_token,
            "created_at": datetime.now(timezone.utc),
            "used": False
        })
        # Return token for testing (in production, this would be emailed)
        return {"message": "Password reset instructions sent", "token": reset_token}
    
    return {"message": "If the email exists, reset instructions have been sent"}

@router.post("/password-reset-confirm")
async def confirm_password_reset(request: PasswordResetConfirm):
    """Confirm password reset with token"""
    email = verify_reset_token(request.token)
    if not email:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    
    # Check if token was already used
    reset_record = await db.password_resets.find_one({"token": request.token, "used": False})
    if not reset_record:
        raise HTTPException(status_code=400, detail="Reset token already used or invalid")
    
    # Update password
    hashed_password = get_password_hash(request.new_password)
    await db.users.update_one(
        {"email": email},
        {"$set": {"hashed_password": hashed_password, "updated_at": datetime.now(timezone.utc)}}
    )
    
    # Mark token as used
    await db.password_resets.update_one({"token": request.token}, {"$set": {"used": True}})
    
    return {"message": "Password reset successful"}

@router.post("/change-password")
async def change_password(request: ChangePasswordRequest, current_user: dict = Depends(get_current_user)):
    """Change password for logged-in user"""
    user = await db.users.find_one({"email": current_user["email"]})
    
    if not verify_password(request.current_password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    
    hashed_password = get_password_hash(request.new_password)
    await db.users.update_one(
        {"email": current_user["email"]},
        {"$set": {"hashed_password": hashed_password, "updated_at": datetime.now(timezone.utc)}}
    )
    
    return {"message": "Password changed successfully"}

# Admin routes for user management
@router.get("/users")
async def list_users(current_user: dict = Depends(require_admin)):
    """List all users (admin only)"""
    users = await db.users.find({}, {"_id": 0, "hashed_password": 0}).to_list(length=1000)
    return users

@router.post("/users", response_model=UserResponse)
async def create_user(user: UserCreate, current_user: dict = Depends(require_admin)):
    """Create a new user (admin only)"""
    # Check if user exists
    existing = await db.users.find_one({"email": user.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    
    user_data = {
        "id": user_id,
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role.value,
        "district_code": user.district_code,
        "is_active": user.is_active,
        "hashed_password": get_password_hash(user.password),
        "created_at": now,
        "updated_at": now,
        "created_by": current_user["user_id"]
    }
    
    await db.users.insert_one(user_data)
    
    return UserResponse(
        id=user_id,
        email=user.email,
        full_name=user.full_name,
        role=user.role,
        district_code=user.district_code,
        is_active=user.is_active,
        created_at=now
    )

@router.put("/users/{user_id}")
async def update_user(user_id: str, update: UserUpdate, current_user: dict = Depends(require_admin)):
    """Update a user (admin only)"""
    user = await db.users.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    update_data = {k: v for k, v in update.dict().items() if v is not None}
    if update_data.get("role"):
        update_data["role"] = update_data["role"].value
    update_data["updated_at"] = datetime.now(timezone.utc)
    
    await db.users.update_one({"id": user_id}, {"$set": update_data})
    
    updated_user = await db.users.find_one({"id": user_id}, {"_id": 0, "hashed_password": 0})
    return updated_user

@router.delete("/users/{user_id}")
async def delete_user(user_id: str, current_user: dict = Depends(require_admin)):
    """Delete a user (admin only)"""
    if user_id == current_user["user_id"]:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
    
    result = await db.users.delete_one({"id": user_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": "User deleted successfully"}

@router.post("/users/{user_id}/reset-password")
async def admin_reset_password(user_id: str, current_user: dict = Depends(require_admin)):
    """Reset user password to a temporary one (admin only)"""
    user = await db.users.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Generate temporary password
    temp_password = str(uuid.uuid4())[:12]
    hashed_password = get_password_hash(temp_password)
    
    await db.users.update_one(
        {"id": user_id},
        {"$set": {"hashed_password": hashed_password, "updated_at": datetime.now(timezone.utc)}}
    )
    
    return {"message": "Password reset successful", "temporary_password": temp_password}

# Initialize default admin user
async def create_default_admin(database):
    """Create default admin user if none exists"""
    global db
    db = database
    
    admin_exists = await db.users.find_one({"role": "admin"})
    if not admin_exists:
        admin_user = {
            "id": str(uuid.uuid4()),
            "email": "admin@mahaedume.gov.in",
            "full_name": "System Administrator",
            "role": "admin",
            "is_active": True,
            "hashed_password": get_password_hash("admin123"),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        await db.users.insert_one(admin_user)
        print("Default admin user created: admin@mahaedume.gov.in / admin123")
