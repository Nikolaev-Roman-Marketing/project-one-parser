import os
import time
from selenium import webdriver


class Downloader:

    def __init__(self):

        dir_path = os.getcwd()
        self.driver = webdriver.Firefox(executable_path=dir_path + "\\geckodriver.exe")

    def get_page(self, url):
        """
        Переходим на страницу url.
        И проматываем ее в самый низ, чтобы прогрузить весь код.
        """

        self.driver.get(url)
        time.sleep(7)
        page_bottom = self.driver.execute_script("return document.body.scrollHeight")
        self.driver.execute_script(f"window.scrollTo(0, {page_bottom})")

    def get_code(self):
        """
        Получаем код страницы.
        """

        time.sleep(0.2)
        code = self.driver.page_source
        return code

    def run(self, url):

        self.get_page(url)
        code = self.get_code()

        return code


if __name__ == '__main__':

    downloader = Downloader()
    
