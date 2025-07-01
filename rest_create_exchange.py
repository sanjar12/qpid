#!/usr/bin/env python3

import requests
import json
import argparse
import sys

BROKER_REST_URL = "http://localhost:8080"  # Default REST management port

def create_exchange_rest(exchange_name, exchange_type, properties):
    url = f"{BROKER_REST_URL}/api/latest/exchange"
    
    data = {
        "name": exchange_name,
        "type": exchange_type,
        **properties
    }
    
    try:
        response = requests.put(f"{url}/{exchange_name}", json=data)
        
        if response.status_code in [200, 201]:
            print(f"[SUCCESS] Exchange '{exchange_name}' created successfully")
            return True
        else:
            print(f"[ERROR] Failed to create exchange. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("[ERROR] Could not connect to broker REST interface")
        print("Make sure the broker has REST management enabled")
        return False
    except Exception as e:
        print(f"[ERROR] {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create exchange via REST API")
    parser.add_argument("exchange_type", help="Exchange type")
    parser.add_argument("exchange_name", help="Exchange name")
    parser.add_argument("--durable", action="store_true", help="Durable exchange")
    
    opts = parser.parse_args()
    
    properties = {
        "durable": opts.durable
    }
    
    create_exchange_rest(opts.exchange_name, opts.exchange_type, properties)
