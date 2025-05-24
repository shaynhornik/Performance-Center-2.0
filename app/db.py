import os
from supabase import create_client, Client

def get_supabase() -> Client:
    # Using a singleton pattern so we don't open new connections each request
    if not hasattr(get_supabase, "_client"):
        url  = os.environ["SUPABASE_URL"]
        key  = os.environ["SUPABASE_SERVICE_KEY"]
        get_supabase._client = create_client(url, key)
    return get_supabase._client
