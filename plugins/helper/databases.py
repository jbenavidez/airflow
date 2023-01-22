from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from airflow.hooks.postgres_hook import PostgresHook

import os 

currect_dir = os.path.dirname(__file__)

postgres_hook = PostgresHook(postgres_conn_id='postgres')

engine = postgres_hook.get_sqlalchemy_engine()
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

def init_db():
    Base.metadata.create_all(bind=engine)