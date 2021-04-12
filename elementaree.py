import pandas as pd
from bs4 import BeautifulSoup
import time
from downloader import Downloader
from connector import Connector


class Elementaree:

    def __init__(self):

        url = "https://elementaree.ru/menu"

        self.downloader = Downloader()
        self.soup = BeautifulSoup(self.downloader.run(url), 'lxml')

        self.data = {'week': [], 'number': [],
                     'category': [], 'name': [],
                     'price': [], 'mass': [],
                     'min_persons': [], 'time': [],
                     'ingredients': [], 'kbzhu': []}

    def get_name(self, code_block):
        """
        Собираем имя блюда, состоящее из нескольких строк.
        """

        name = ""
        for row in code_block.find_all(['div', 'p'], {'class': 'dish-card-title__text___1RYgp'}):

            if row.text is not None:
                name += str(row.text) + " "

        return name[0:-1]

    def get_kbzhu(self, code):
        """
        Собираем КБЖУ из разных блоков.
        """

        kbzhu = ''

        for nutrition in code.find_all('div', {'class': 'dish-details__nutrition-item___LuKWp'}):

            if nutrition is not None:
                name = nutrition.find('div', {'class': 'dish-details__nutrition-item-title___1BkEJ'}).text
                value = nutrition.find('div', {'class': 'dish-details__nutrition-item-value___3z3sQ'}).text
                kbzhu += str(name) + ": " + str(value) + ", "

        return kbzhu[0:-2]

    def get_cook_time(self, code):
        """
        Ищем время приготовления. Если есть.
        """

        cook_time = code.find('div', {'class': 'dish-details__content-cooktime-value___2xIKk'})

        if cook_time is not None:
            cook_time = cook_time.text
        else:
            cook_time = '0'

        return cook_time

    def get_dish_page_code(self, link):
        """
        Переходим на отдельную страницу блюда и получаем её код
        """

        self.downloader.driver.get(link)
        time.sleep(3.5)
        code = BeautifulSoup(self.downloader.get_code(), 'lxml')

        return code

    def create_dataset(self):

        complete_data = pd.DataFrame(self.data)
        complete_data['competitor'] = 'elementaree'

        return complete_data

    def send_to_mysql(self):
        """
        Проверяем данные на дубликаты и отправляем в базу данных.
        """

        c = Connector()
        complete_data = self.create_dataset()
        complete_data = c.clear_duplicates(complete_data)
        c.send_data(complete_data)

    def parse(self):
        """
        Ищем все категории меню.
        Ищем все блюда по каждой категории.
        Парсим всё необходимое по каждому блюду.
        """

        menu = self.soup.find_all('div', {'class': 'menu-category-container___1oJJh'})

        week = self.soup.find('div', {'filter-item__subtitle___1_k3n exited___2WOp8'}).text

        dish_number = 1
        for category in menu[0]:

            # ищем все категории в текущем меню
            category_name_container = category.find('p', {'class': 'category__title___2SalO'})

            if category_name_container is not None:

                category_name = category_name_container.text
                
                # эта категория нам не нужна
                if category_name != "Приятные мелочи":

                    # итерируемся по всем карточкам блюд
                    for dish_container in category.find_all('li', {'class': 'dish-card___2rD2O'}):

                        try:

                            dish_link = dish_container.find('a', {'class': 'dish-card__image--container___1EpnN'},
                                                            href=True)['href']

                            dish_name = self.get_name(dish_container)
                            dish_price = dish_container.find('p', {'class': 'dish-card__price___3D1rw'}).text[0:-1]
                            dish_person = dish_container.find('span', {'class': 'portions__count___ikw4L'}).text
                            dish_mass = dish_container.find('span', {'class': 'portions__size___1SxUs'}).text[0:-1]

                            dish_page_code = self.get_dish_page_code(dish_link)

                            dish_ingrid = dish_page_code.find('div', {'class': 'dish-details__ingredients-text___3Zcw9'}).text
                            dish_kbzhu = self.get_kbzhu(dish_page_code)
                            dish_time = self.get_cook_time(dish_page_code)

                            dish = {'week': week, 'number': dish_number,
                                    'category': category_name,
                                    'name': dish_name, 'price': dish_price,
                                    'mass': int(dish_mass)/int(dish_person), 'min_persons': dish_person,
                                    'time': dish_time,
                                    'ingredients': dish_ingrid, 'kbzhu': dish_kbzhu}

                            for key in self.data.keys():
                                self.data[key]. append(dish[key])

                        except BaseException as err:
                            print(f"Ошибка загрузки товара - {err}")

                        dish_number += 1

        self.send_to_mysql()
        self.downloader.driver.quit()
        print("Парсинг Elementaree окончен")


if __name__ == '__main__':

    ele = Elementaree()
    ele.parse()
