import os
import time
import requests
from io import StringIO
from queue import Queue
from threading import Thread, Lock
from bs4 import BeautifulSoup
from selenium import webdriver
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter


class AdvancedMicrosoftAcademicCrawler:

    def __init__(self, sleep, cpu_core):
        self.sleep = sleep
        self.cpu_core = cpu_core
        self.folder = './download_pdf/'
        self.pdf = []
        self.marker = 0
        self.lock = Lock()
        self.queue = Queue()
        self.queue_2 = Queue()
        self.queue_3 = Queue()
        self.queue_4 = Queue()

    def extract(self, query, page_num):
        pages = self.fetch_search_result_urls(query, page_num)
        for page in pages:
            self.queue.put(page)
        for i in range(self.cpu_core * 4):
            t = Thread(target=self.do_job_helper_1)
            t.daemon = True
            t.start()
        self.queue.join()
        self.fetch_pdf_links()
        self.download_pdf()
        self.convert_pdf_to_txt()

    def do_job_helper_1(self):
        while True:
            page = self.queue.get()
            for t in self.fetch_paper_links(page):
                self.queue_2.put(t)
            self.queue.task_done()

    def fetch_search_result_urls(self, query, page_num):
        result = []
        query = query.lower().split(' ')
        real_query = '%20'.join(query)
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

    def fetch_pdf_links(self):
        for i in range(self.cpu_core * 4):
            t = Thread(target=self.do_job_helper_2)
            t.daemon = True
            t.start()
        self.queue_2.join()

    def do_job_helper_2(self):
        while True:
            paper = self.queue_2.get()
            try:
                self.pdf(paper)
            except:
                pass
            self.queue_2.task_done()

    def pdf(self, url):
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        driver = webdriver.Chrome(chrome_options=option)
        driver.get(url)
        time.sleep(self.sleep)
        raw_html = driver.page_source
        driver.quit()
        soup1 = BeautifulSoup(raw_html, 'html.parser')
        div_layer = soup1.find_all('div', class_='ma-link-collection')
        zero_div = str(div_layer[0])
        if 'View PDF:' in zero_div:
            soup1 = BeautifulSoup(zero_div, 'html.parser')
            a_tags = soup1.find_all('a')
            if a_tags is not None:
                try:
                    self.pdf.append(a_tags[0]['href'])
                except:
                    pass

    def download_pdf(self):
        folder = os.path.exists(self.folder)
        if not folder:
            os.makedirs(self.folder)
        for pdf in self.pdf:
            self.queue_3.put(pdf)
        for i in range(self.cpu_core * 4):
            t = Thread(target=self.do_job_helper_3)
            t.daemon = True
            t.start()
        self.queue_3.join()

    def do_job_helper_3(self):
        while True:
            pdf = self.queue_3.get()
            try:
                r = requests.get(pdf, timeout=15)
                self.lock.acquire()
                with open(self.folder + str(self.marker) + '.pdf', 'wb') as code:
                    code.write(r.content)
                self.marker += 1
            except:
                pass
            finally:
                self.lock.release()
            self.queue_3.task_done()

    def convert_pdf_to_txt(self):
        for file in os.listdir(self.folder):
            self.queue_4.put(file)
        for i in range(self.cpu_core * 4):
            t = Thread(target=self.do_job_helper_4)
            t.daemon = True
            t.start()
        self.queue_4.join()

    def do_job_helper_4(self):
        while True:
            file = self.queue_4.get()
            try:
                f = open(self.folder + file[:-4] + '.txt', 'w', encoding='utf-8')
                f.write(self.convert(self.folder + file))
                f.close()
                os.remove(self.folder + file)
            except:
                pass
            self.queue_4.task_done()

    def convert(self, fname, pages=None):
        if not pages:
            pagenums = set()
        else:
            pagenums = set(pages)
        output = StringIO()
        manager = PDFResourceManager()
        converter = TextConverter(manager, output, laparams=LAParams())
        interpreter = PDFPageInterpreter(manager, converter)
        infile = open(fname, 'rb')
        for page in PDFPage.get_pages(infile, pagenums):
            try:
                interpreter.process_page(page)
            except:
                continue
        infile.close()
        converter.close()
        text = output.getvalue()
        output.close()
        return text
