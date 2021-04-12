import pandas as pd
from bs4 import BeautifulSoup
import time
from downloader import Downloader
from connector import Connector


class Chefmarket:

    def __init__(self):

        self.url = "https://www.chefmarket.ru"

        self.categories = ['original', 'family', '20minutes', 'balance']

        self.downloader = Downloader()

        self.data = {'week': [],
                     'number': [], 'category': [],
                     'name': [], 'price': [],
                     'mass': [], 'min_persons': [],
                     'time': [],
                     'ingredients': [], 'kbzhu': []}

    def get_ingredients(self, code):
        """
        Формируем список ингредиентов через запятую
        """

        ingredients = ""
        for ingrid in code.find_all('div', {'class': 'dishComponent-name mt-4 px-16'}):
            text = ingrid.find('div', {'class': 'dishComponent-name-text dishComponent-name-bordered'}).text
            ingredients += text + ", "

        return ingredients[0:-2]

    def get_dish_page_code(self, link):
        """
        Получаем полный код страницы с блюдом.
        """

        self.downloader.driver.get(link)
        time.sleep(3.1)
        code = BeautifulSoup(self.downloader.get_code(), 'lxml')

        return code

    def create_dataset(self):

        complete_data = pd.DataFrame(self.data)
        complete_data['mass'] = complete_data['mass'].astype(int)
        complete_data['mass'] = complete_data['mass'] // 2
        complete_data['competitor'] = 'chefmarket'

        return complete_data

    def send_to_mysql(self):
        """
        Собираем из словаря таблицу.
        Проверяем данные на дубликаты и отправляем в базу данных.
        """

        c = Connector()

        complete_data = self.create_dataset()
        complete_data = c.clear_duplicates(complete_data)
        c.send_data(complete_data)

    def parse(self):
        """
        Переходим на страницу с меню каждой категории.
        Ищем все блюда для категории.
        Собираем все нужные данные.
        """

        dish_number = 1
        for category_name in self.categories:

            soup = BeautifulSoup(self.downloader.run(self.url + "/5dinners-" + category_name), 'lxml')
            menu = soup.find_all('div', {'class': 'pb-16 col-sm-12 col-md-6 col-lg-6 col-xl-4 no-pads-sm'})
            week = soup.find('div', {'class': 'heading heading-xl-s menu-dates-title text-center'}).text

            for dish_container in menu:

                try:

                    dish_link = self.url + dish_container.find('a', href=True)['href']
                    dish_name = dish_container.find('div', {'col-12 title pb-16 px-8 mx-0 px-md-24 pt-md-8'}).text
                    dish_mass = dish_container.find('div', {'col-auto stat ml-0'}).text[0:-2]
                    dish_time = dish_container.find('div', {'col-auto stat'}).text[0:-4]

                    dish_page_code = self.get_dish_page_code(dish_link)

                    dish_ingrid = self.get_ingredients(dish_page_code)
                    dish_kbzhu = dish_page_code.find('div', {'class': 'dish-value'}).text

                    dish = {'week': week, 'number': dish_number,
                            'category': category_name,
                            'name': dish_name, 'price': "",
                            'mass': dish_mass, 'min_persons': "",
                            'time': dish_time,
                            'ingredients': dish_ingrid, 'kbzhu': dish_kbzhu}

                    # удалеям лишние пробелы и сохраняем данные в главный словарь
                    for key in self.data.keys():

                        element = dish[key]

                        if key == "number":
                            pass
                        else:
                            element = element.replace('\n', '')
                            element = ' '.join(element.split())

                        self.data[key].append(element)

                except BaseException as err:
                    print(f"Ошибка загрузки товара - {err}")

                dish_number += 1

        self.send_to_mysql()
        self.downloader.driver.quit()
        print("Парсинг Chefmarket окончен")


if __name__ == '__main__':

    chef = Chefmarket()
    chef.parse()
    
