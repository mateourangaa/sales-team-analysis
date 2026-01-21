import os
import pandas as pd
from faker import Faker
from supabase import create_client
import random
import string
from datetime import datetime, timedelta
from dotenv import load_dotenv


# 1. CREDENTIALS
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

fake = Faker()

# 2. LOAD DATA
agents_list = pd.read_csv('sales_teams.csv')['sales_agent'].tolist()
accounts_list = pd.read_csv('accounts.csv')['account'].tolist()
products = ['GTX Plus Basic', 'GTXPro', 'MG Special', 'GTX Basic', 'MG Advanced']
STAGES = ['Prospecting', 'Engaging', 'Won', 'Lost']

def generate_opportunity_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def run_backfill(count=200):
    print(f"Starting backfill of {count} historical records...")
    new_records = []
    
    for i in range(count):
        # SPREAD DATES: Pick a random day in the last 90 days
        days_ago = random.randint(0, 90)
        engage_date = datetime.now() - timedelta(days=days_ago)
        
        # SPREAD STAGES: Most are Prospecting/Engaging, some are Won/Lost
        stage = random.choices(STAGES, weights=[50, 30, 5, 15], k=1)[0]
        
        record = {
            "opportunity_id": generate_opportunity_id(),
            "sales_agent": random.choice(agents_list),
            "product": random.choice(products),
            "account": random.choice(accounts_list),
            "deal_stage": stage,
            "engage_date": engage_date.strftime("%Y-%m-%d"),
            "close_value": random.randint(500, 12000)
        }
        
        # LOGICAL CLOSE DATES: If it's Won/Lost, it needs a close date after engagement
        if stage in ['Won', 'Lost']:
            duration = random.randint(5, 30)
            close_date = engage_date + timedelta(days=duration)
            # Ensure we don't close in the future relative to today
            if close_date > datetime.now():
                close_date = datetime.now()
            record["close_date"] = close_date.strftime("%Y-%m-%d")

        new_records.append(record)
        
        # Push in batches of 50 to avoid Supabase timeouts
        if len(new_records) == 50:
            supabase.table('pipeline').insert(new_records).execute()
            new_records = []
            print(f"pushed batch {i+1}...")

    # Push any remaining records
    if new_records:
        supabase.table('pipeline').insert(new_records).execute()
    
    print("✅ Backfill Complete! Your database now has natural history.")

if __name__ == "__main__":
    run_backfill(200) # You can change this number
