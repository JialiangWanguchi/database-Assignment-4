#This file is used to set up the database connections and the ORM-to-database "handles".

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# 1. set MySQL Connection
mysql_url = "mysql+pymysql://root:123456Qwerdf@localhost:3306/sakila"
mysql_engine = create_engine(mysql_url)

# 2. define SQLite Connection
sqlite_url = "sqlite:///analytics.db"
sqlite_engine = create_engine(sqlite_url)

# 3. create base classes
SakilaBase = declarative_base()
AnalyticsBase = declarative_base()

# 4. create session "factories"
SakilaSession = sessionmaker(bind=mysql_engine)
AnalyticsSession = sessionmaker(bind=sqlite_engine)
