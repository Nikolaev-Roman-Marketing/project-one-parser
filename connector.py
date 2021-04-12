import pandas as pd
from sqlalchemy import create_engine
import pymysql
import json


class Connector:

    def __init__(self):
        
        # получаем логин, пароль, назвние базы данных и нужной таблицы 
        with open('secrets.json', 'r') as file:
            secrets = json.load(d)
            self.login = secrets['login']
            self.password = secrets['password']
            self.database = secrets['database']
            self.table = secrets['table']
            file.close()

        self.conn = create_engine(f'mysql+pymysql://{self.login}:{self.password}'
                                  f'@localhost/{self.database}', echo=False)

    def clear_duplicates(self, data):
        
        """
        Берем из базы данных последние данные
        Сверяем с новыми данными и удаляем дубликаты если есть
        """
        
        last_actual_data = pd.read_sql(f'SELECT week, number, competitor '
                                       f'FROM {self.table} '
                                       f'ORDER BY id '
                                       f'LIMIT 1000 ',
                                       self.conn)

        data = pd.merge(data, last_actual_data, on=['week', 'number', 'competitor'], how='left', indicator=True)
        data = data.query('_merge == "left_only"').drop(['_merge'], axis=1)

        return data

    def send_data(self, data):

        data.to_sql(name=self.table, con=self.conn, if_exists='append', index=False)


if __name__ == '__main__':

    with open('secrets.json', 'r') as d:
        secrets = json.load(d)
        login = secrets['login']
        password = secrets['password']
        database = secrets['database']
        table = secrets['table']
        d.close()

    conn = create_engine(f'mysql+pymysql://{login}:{password}'
                         f'@localhost/{database}', echo=False)

    last_actual_data = pd.read_sql(f'SELECT week, number '
                                   f'FROM {table} '
                                   f'ORDER BY id '
                                   f'LIMIT 1000 ',
                                   conn)

    print(last_actual_data)
