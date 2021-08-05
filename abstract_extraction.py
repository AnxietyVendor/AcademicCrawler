import urllib.request
from bs4 import BeautifulSoup


class abstract_extraction:

    def extract(self, query, page_num):
        result = []
        pages = []
        resource_pages = self.fetch_resource_pages(query, page_num)
        for resource_page in resource_pages:
            pages += self.fetch_single_resource_page(resource_page)
        for page in pages:
            tmp = self.fetch_abstract(page)
            if (tmp is not None) and (tmp not in result):
                result.append(tmp)
        return result

    def fetch_resource_pages(self, query, page_num):
        resource_pages = []
        queries = query.lower().split(' ')
        real_query = '+'.join(queries)
        for i in range(page_num):
            resource_pages.append('https://cn.bing.com/academic/search?q=' + real_query + '&first=' + str(i) + '1')
        return resource_pages

    def fetch_single_resource_page(self, link):
        result = []
        headers = ('User-Agent',
                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36')
        opener = urllib.request.build_opener()
        opener.addheaders = [headers]
        mybytes = opener.open(link, timeout=10).read()
        raw_html = mybytes.decode('utf8')
        soup = BeautifulSoup(raw_html, "html.parser")
        h2_labels = soup.find_all('h2')
        for label in h2_labels:
            if label.find('a') is not None:
                url = label.find('a')['href']
                if url.startswith('http'):
                    result.append(url)
                else:
                    result.append('https://cn.bing.com' + url)
        return result

    def fetch_abstract(self, url):
        try:
            headers = ('User-Agent',
                       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36')
            opener = urllib.request.build_opener()
            opener.addheaders = [headers]
            mybytes = opener.open(url, timeout=10).read()
            raw_html = mybytes.decode('utf8')
            soup = BeautifulSoup(raw_html, "html.parser")
            label_1 = soup.find('li', class_='aca_main')
            if label_1 is None:
                return None
            soup = BeautifulSoup(str(label_1), "html.parser")
            label_2 = soup.find_all('div', class_='aca_desc b_snippet')
            if label_2 is None:
                return None
            soup = BeautifulSoup(str(label_2[1]), "html.parser")
            label_3 = soup.find('span')
            if label_3 is None:
                return None
            for child in label_3.children:
                return child['title']
        except:
            return None
