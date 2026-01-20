import os
import pandas as pd
from supabase import create_client
import random
from faker import Faker
import string
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

fake = Faker()
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

STAGES = ['Prospecting', 'Engaging', 'Won', 'Lost']


# load data
agents_list = pd.read_csv('sales_teams.csv')['sales_agent'].tolist()
accounts_list = pd.read_csv('accounts.csv')['account'].tolist()
products = ['GTX Plus Basic', 'GTXPro', 'MG Special', 'GTX Basic', 'MG Advanced']



# generates random opportunity id
def generate_opportunity_id():
    """Generates a random ID like '1C1I7A6R'"""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))



# generates new sales data
def push_new_sales(count=5):
    """Generates and sends new sales to Supabase"""
    new_records = []
    
    for _ in range(count):
        record = {
            "opportunity_id": generate_opportunity_id(),
            "sales_agent": random.choice(agents_list),
            "product": random.choice(products),
            "account": random.choice(accounts_list),
            "deal_stage": "Prospecting",
            "engage_date": datetime.now().strftime("%Y-%m-%d"),
            "close_value": random.randint(500, 12000)
        }
        new_records.append(record)
    
    try:
        supabase.table('pipeline').insert(new_records).execute()
    except Exception as e:
        print(f"Error: {e}")




# progresses existing deals to later stages
def progress_existing_deals():
    """Finds 'open' deals and moves them to the next stage"""
    # 1. Fetch some open deals from Supabase
    response = supabase.table('pipeline') \
        .select("opportunity_id, deal_stage") \
        .in_("deal_stage", ['Prospecting', 'Engaging']) \
        .limit(30).execute()
    
    deals_to_update = response.data
    
    for deal in deals_to_update:
        current_stage = deal['deal_stage']
        # chance a deal moves forward
        if random.random() > 0.3:
            # If at Engaging stage, randomly choose Won or Lost
            if current_stage == 'Engaging':
                new_stage = random.choice(['Won', 'Lost'])
            else:
                new_stage = STAGES[STAGES.index(current_stage) + 1]
            
            update_data = {"deal_stage": new_stage}

            # Update engage_date when moving TO Engaging stage
            if new_stage == 'Engaging':
                update_data["engage_date"] = datetime.now().strftime("%Y-%m-%d")
            # If the deal is now Closed (Won/Lost), add a close date
            if new_stage in ['Won', 'Lost']:
                update_data["close_date"] = datetime.now().strftime("%Y-%m-%d")
            
            supabase.table('pipeline') \
                .update(update_data) \
                .eq("opportunity_id", deal['opportunity_id']) \
                .execute()
            




push_new_sales(5)

progress_existing_deals() 


