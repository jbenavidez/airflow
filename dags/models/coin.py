from sqlalchemy import Column, Integer, String, DateTime, Float, Text
from typing import Dict
# from .databases import Base, db_session
from helper.databases import Base
import traceback
import logging


class Coin(Base):
    __tablename__ = 'coins'
    __table_args__ = {'extend_existing': True} 
    id = Column(Integer , primary_key=True)
    name = Column(Text)
    symbol = Column(Text)
    current_price = Column(Float,  nullable = False)
    high_24h = Column(Float,  nullable = False)
    low_24h = Column(Float,  nullable = False)

    @classmethod
    def get_all_coins(cls) -> dict:
        """ This func get all coins from  our db."""
        coin_list = cls.query.all()
        return [cls.entity_to_obj(item) for item in coin_list]

    @classmethod
    def create(cls, coin_info:dict) -> Dict:
        # Save coin if does exist
        coin_name = coin_info['name']
        # Check is coin exist: 
        coin_checker = cls.get_coin_by_symbol(coin_info['symbol'])
        if coin_checker is not None:
            logging.info(f"coin already exist {coin_name}")
            return coin_info

        try:
            logging.info(f"saving coin {coin_name}...")
            entry = cls(
                    name = coin_name,
                    symbol = coin_info['symbol'],
                    current_price = coin_info['current_price'],
                    high_24h = coin_info['high_24h'],
                    low_24h = coin_info['low_24h'],
                    # last_updated = coin_info['last_updated'],
                    # atl_date = coin_info['atl_date'],
                    )
            db_session.add(entry)
            db_session.commit()
        except Exception as e:
            logging.error(f"coin could not be store {coin_name}. \
                            Error {e} .traceback: {traceback.format_exc()}")
        return coin_info

    @classmethod
    def get_coin_by_symbol(cls, symbol: str) -> dict:
        # get coin by symbol
        return cls.query.filter(cls.symbol == symbol).first()

    @classmethod
    def entity_to_obj(cls, coin_entity) -> Dict:
        """convert entity to object"""
        return {
                "id": coin_entity.id,
                "name": coin_entity.name,
                "symbol": coin_entity.symbol,
                "current_price": coin_entity.current_price,
                "high_24h": coin_entity.high_24h,
                "low_24h": coin_entity.low_24h,
                }