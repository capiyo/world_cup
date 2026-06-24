import pymongo
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from bson import ObjectId

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "clashdb"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]

print("🚀 Recovering users matching Rust model...")

# ✅ REMOVE the problematic unique index if it exists
try:
    db.users.drop_index("id_1")
    print("✅ Dropped problematic 'id_1' index")
except:
    print("ℹ️ Index 'id_1' doesn't exist or already removed")

# Get existing users (using username as identifier since id might be problematic)
existing_users = set()
for user in db.users.find({}, {"username": 1}):
    if user.get("username"):
        existing_users.add(user["username"])

print(f"📊 Existing users: {len(existing_users)}")

# Get all users from users_profile
profile_users = list(db.user_profiles.find())
print(f"📊 Users in users_profile: {len(profile_users)}")

# Find missing (by username)
missing = [p for p in profile_users if p.get("username") not in existing_users]
print(f"📊 Missing: {len(missing)}")

if missing:
    now = datetime.now(timezone.utc)
    recovered = 0
    
    for profile in missing:
        # Generate a unique ID for this user
        user_id = str(ObjectId())
        
        # Build user document matching Rust model EXACTLY
        user_doc = {
            "id": user_id,  # ✅ Add id field for the unique index
            "username": profile.get("username", f"user_{user_id[:8]}"),
            "phone": profile.get("phone", ""),
            "balance": profile.get("balance", 0.0),
            "created_at": profile.get("created_at", now),
            "updated_at": profile.get("updated_at", now),
            "is_admin": False,
            "season_points": 0,
            "correct_votes": 0,
            "total_votes": 0,
            "pin_hash": None,
            "pin_salt": None,
            "is_pin_enabled": False,
            "firebase_uid": None,
            "auth_methods": [],
            "last_login": None
        }
        
        # Insert using the id field to avoid duplicate key errors
        db.users.update_one(
            {"id": user_id},
            {"$set": user_doc},
            upsert=True
        )
        recovered += 1
        print(f"  ✅ Recovered: {profile.get('username')}")
    
    print(f"\n✅ Recovered {recovered} users")
else:
    print("✅ No missing users")

print(f"📊 Total users: {db.users.count_documents({})}")

# Show sample
print("\n📋 Sample recovered users:")
for user in db.users.find().limit(5):
    print(f"  - {user.get('username')} (id: {user.get('id')})")

client.close()