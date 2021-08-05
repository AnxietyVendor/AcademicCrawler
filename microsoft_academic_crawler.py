import time
import warnings
from selenium import webdriver
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')
MULTI_THREAD_MODE = True


class microsoft_academic_crawler:

    def __init__(self, sleep, cpu_core):
        self.sleep = sleep
        self.cpu_core = cpu_core
        self.result = []

    def extract(self, query, page_num):
        search_result_urls = self.fetch_search_result_urls(query, page_num)
        paper_links = []
        for result_url in search_result_urls:
            paper_links.extend(self.fetch_paper_links(result_url))
        count = 0
        pool = ThreadPoolExecutor(self.cpu_core * 4)
        for paper in paper_links:
            try:
                if MULTI_THREAD_MODE:
                    pool.submit(self.task, paper)
                else:
                    self.task(paper)
            except:
                continue
        pool.shutdown()

    def task(self, paper):
        abstract, keywords = self.fetch_abstract_and_keywords(paper)
        self.result.append(abstract)

    def fetch_search_result_urls(self, query, page_num):
        query = query.lower().split(' ')
        real_query = '%20'.join(query)
        result = []
        for i in range(page_num):
            result.append('https://academic.microsoft.com/search?q=' + real_query + '&f=&orderBy=0&skip=' + str(
                i * 10) + '&take=10')
        return result

    def fetch_paper_links(self, url):
        result = []
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        driver = webdriver.Chrome(chrome_options=option)
        driver.get(url)
        time.sleep(self.sleep)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        labels = soup.find_all('a', class_='title au-target')
        driver.quit()
        for label in labels:
            result.append('https://academic.microsoft.com/' + label['href'])
        return result

    def fetch_abstract_and_keywords(self, url):
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        driver = webdriver.Chrome(chrome_options=option)
        driver.get(url)
        time.sleep(self.sleep)
        raw_html = driver.page_source
        driver.quit()
        soup1 = BeautifulSoup(raw_html, 'html.parser')
        div_layer = soup1.find('div', class_='name-section')
        soup2 = BeautifulSoup(str(div_layer), 'html.parser')
        abstract = soup2.find('p').get_text()
        key_words = []
        kw_div_layer = soup1.find('div', class_='edp-layout')
        soup3 = BeautifulSoup(str(kw_div_layer), 'html.parser')
        kw_labels = soup3.find_all('div', class_='text')
        for kw_label in kw_labels:
            key_words.append(kw_label.get_text().strip())
        return abstract, key_words

    def get_res(self, sleep_time):
        time.sleep(sleep_time)
        return self.result
