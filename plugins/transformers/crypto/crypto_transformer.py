

from services.http_services import HttpServices
from models.coin import Coin
from typing import List, Dict, Union
import logging, traceback
import statistics


class CryptoTransformer:

    @staticmethod
    def load_coins() -> List[Dict]:
        """Load coins"""
        logging.info("init loading coin")
        coins = HttpServices.get_coins()
        logging.info("login coins complete")
        return coins

    @staticmethod
    def get_top_three_coins(ti:any) -> List[dict]:
        coins = ti.xcom_pull(task_ids="load_coins")
        # Get top 3 coins
        sorted_coins = sorted(coins, key= lambda k:k['current_price'], reverse=True)
        return sorted_coins[:3] # return top 3

    @staticmethod
    def insert_coin_in_db(ti:any) -> None:
        """ ti = task instance"""
        coins = ti.xcom_pull(task_ids="get_top_three_coins")
        #for coin in coins:
        for coin in coins:
            Coin.create(coin)
        print("data loading completed", coins) 

    @classmethod
    def _get_avg_price(cls, coin_obj: Dict) -> Union[Dict, None]:
        """Calculate avg price."""
        coin_id = coin_obj['id']
        logging.info(f"calculate_avg_price {coin_id}")
        current_price = coin_obj['current_price']
        coin_history = HttpServices.get_coin_price_history(coin_id)
        if coin_history is None:
            return 
        coins_price = [x[1] for x in coin_history]
        avg_price =  statistics.mean(coins_price)
        try:
            coin_obj = {
                        "id":coin_id,
                        "symbol":coin_obj['symbol'],
                        "current_price":current_price,
                        "is_valid_to_buy": False,
                        "avg_price":avg_price
                        }
            logging.info(f"Checking price for {coin_id}")
            if current_price < avg_price:
                coin_obj["is_valid_to_buy"] = True
            return coin_obj
        except Exception as e:
            logging.error(f"trade could not be store. Error {e}, Error line {traceback.format_exc()}")
            return 

    @classmethod
    def cal_avg_price(cls, ti:any) -> List[Dict]:
        """ ti = task instance"""
        coins = ti.xcom_pull(task_ids="load_coins")
        must_buy_coins =[]
        for coin in coins:
            check_coin = cls._get_avg_price(coin)
            if check_coin is not None and check_coin.get('is_valid_to_buy'):
                must_buy_coins.append(coin)

        return must_buy_coins
    
    @staticmethod
    def place_order(ti: any) -> List[dict]:
        """ Place Order"""
        must_buy_coins = ti.xcom_pull(task_ids="cal_avg_price_using_coins_historical_data")
        order_records = []
        for coin in must_buy_coins:
            res = HttpServices.submit_order(coin, 3)
            order_records.append(res)
        return order_records


    @staticmethod
    def save_order_transactions(ti: any) -> bool:
        """    """
        must_buy_coins = ti.xcom_pull(task_ids="place_order")
        return True

    @staticmethod
    def send_email(ti: any) -> bool:
        """    """
        must_buy_coins = ti.xcom_pull(task_ids="place_order")
        is_email_send = True  # mocking email sender :P 
        return is_email_send