import pymongo
import os
import random
import string
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "clashdb"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
channels = db["channels"]

# Generate random 6-character invite code
def generate_invite_code():
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choices(chars, k=6))

# Find channels with empty or missing invite_code
empty_channels = channels.find({
    '$or': [
        {'invite_code': ''},
        {'invite_code': {'$exists': False}},
        {'invite_code': None}
    ]
})

# Count how many need updating
count = 0
for channel in empty_channels:
    count += 1
    new_code = generate_invite_code()
    
    # Update the channel with new invite code
    result = channels.update_one(
        {'_id': channel['_id']},
        {'$set': {'invite_code': new_code}}
    )
    
    print(f"✅ {channel.get('name', 'Unknown')} → {new_code}")

print(f"\n🎉 Updated {count} channels with invite codes!")

# ✅ FIX: Use count_documents() instead of count()
still_empty = channels.count_documents({'invite_code': ''})
print(f"📊 Channels still empty: {still_empty}")

client.close()