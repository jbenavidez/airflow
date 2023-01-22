from typing import List, Dict

import requests
import logging
import traceback

class HttpServices:
    debts_base_url = "https://my-json-server.typicode.com/druska/"
    coins_base_url = "https://api.coingecko.com/api/v3/coins/"
    
    @classmethod 
    def get_coins(cls) -> List[Dict]:
        """This function will get the top 10 coins at the current time, sorted by market cap in desc order."""
        response = requests.get('{cls.coins_base_url}markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false')
        return response.json()

    @classmethod 
    def get_coin_price_history(cls, coin_id: str) -> List[Dict]:
        response = requests.get(f"{cls.coins_base_url}{coin_id}/market_chart?vs_currency=usd&days=9&interval=daily")
        return response.json().get('prices')

    @staticmethod
    def submit_order(coin_id: str, quantity: int):
        """
        Mock function to submit an order to an exchange. 
        
        Assume order went through successfully and the return value is the price the order was filled at.
        """
        return {
                "coin_id":coin_id, 
                "quantity": quantity,
                "status": "completed"
                 }

    @classmethod 
    def _fetch_data(cls, url: str) -> List[Dict]:
        # this is used fetch list of debt from api
        try:
            response  = requests.get(f"{cls.debts_base_url}{url}")
            logging.info(f"Debts API call succefully {response.status_code}")
        except Exception as e:
            logging.error(f"Debts API Call Failed {e},Error line {traceback.format_exc()}")
        return response.json()

    @classmethod 
    def get_debts(cls) -> Dict:
        """this function is used to validate data and convert list to dict (dicts are faster for data retrieve)"""
        debts = cls._fetch_data("trueaccord-mock-payments-api/debts")
        debts_dict = {}
        for item in debts:
            try:
                debt_id = item["id"]
                if debts_dict.get(debt_id) is None:
                    debts_dict[debt_id] = item
            except Exception as e:
                logging.error(f"Could not validate debt {e},Error line {traceback.format_exc()}")
        return debts_dict


    @classmethod 
    def get_payment_plans(cls) -> Dict:
        """Get paymenr plans"""
        debts = cls._fetch_data("trueaccord-mock-payments-api/payment_plans")
        debts_dict = {}
        for item in debts:
            try:
                debt_id = item["debt_id"]
                logging.info(f"validated payment_plan info {debt_id}")
                if debts_dict.get(debt_id) is None:
                    debts_dict[debt_id] = item

            except Exception as e:
                logging.error(f"Could not validate payment_plan {e},Error line {traceback.format_exc()}")
                  
        return debts_dict
    
    @classmethod 
    def get_payments(cls) -> Dict:
        """Get payments history"""
        debts = cls._fetch_data("trueaccord-mock-payments-api/payments")
        debts_dict = {}
        for item in debts:
            try:
                payment_plan_id = item["payment_plan_id"]
                logging.info(f"validated payment info {payment_plan_id}")
                if debts_dict.get(payment_plan_id) is None:
                    debts_dict[payment_plan_id] = [item]
                else:
                    debts_dict[payment_plan_id].append(item)
            except Exception as e:
                logging.error(f"Could not validate payment {e},Error line {traceback.format_exc()}")

        return debts_dict