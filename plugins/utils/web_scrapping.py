from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import time



class WebScrapper:
    def __init__(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Remote(
            command_executor="http://selenium-hub:4444/wd/hub",
            options=options
        )

    def get_ngetnews(self, pg_num = 2):
        base_url = "https://www.ngetnews.com/news/articleList.html?page={}&total=67933&view_type=sm&box_idxno=0"

        data_set = []

        for page in range(1,pg_num):
            url = base_url.format(page)
            self.driver.get(url)
            time.sleep(1)

            ul_elem = self.driver.find_element(By.CSS_SELECTOR, 'ul.type2')
            li_list = ul_elem.find_elements(By.TAG_NAME, 'li')

            for li in li_list:
                a_tag = li.find_element(By.CSS_SELECTOR, 'a')
                href = a_tag.get_attribute('href')
                web_id = href.split('=')[1]

                article_url = 'https://www.ngetnews.com/news/articleView.html?idxno=' + web_id

                # open new window with article page
                self.driver.execute_script('window.open(arguments[0]);', article_url)
                self.driver.switch_to.window(self.driver.window_handles[1])

                article_soup = bs(self.driver.page_source, 'html.parser')

                category = article_soup.select_one('#articleViewCon > article > header > p > a').get_text(strip=True)
                title = article_soup.select_one('#articleViewCon > article > header > h1').get_text(strip=True)
                date_txt = article_soup.select_one('#articleViewCon > article > header > ul > li:nth-child(2)').get_text(strip=True)
                date_txt = date_txt.split()[1]


                sentences = []

                atc = article_soup.select_one('#article-view-content-div')
                p_tags = atc.find_all('p')

                for p_tag in p_tags:
                    sentences.append(p_tag.get_text(strip = True))

                contents = ' '.join(sentences)

                data_set.append({
                    "News" : "뉴스저널리즘",
                    "Date" : date_txt,
                    "Category" : category,
                    "Title" : title,
                    "Contents" : contents
                })


                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])

        self.driver.quit()

        return data_set
    


    def get_sisanews(self, pg_num = 2):
        base_url = "https://www.sisa-news.com/news/article_list_all.html?page={}"
        data_set = []

        for page in range(1,pg_num):
            url = base_url.format(page)
            self.driver.get(url)
            time.sleep(1)

            # 기사 제목과 링크 추출
    
            ul_elem = self.driver.find_element(By.CSS_SELECTOR, 'ul.art_list_all')
            li_list = ul_elem.find_elements(By.TAG_NAME, 'li')

            for li in li_list:
                a_tags = li.find_elements(By.TAG_NAME, 'a')
                if not a_tags:
                    continue

                href = a_tags[0].get_attribute('href')
                if href and 'no=' in href:
                    parts = href.split('no=')
                    if len(parts) > 1 and parts[1].isdigit():
                        web_id = parts[1]
                
                article_url = 'https://www.sisa-news.com/news/article.html?no=' + web_id

                self.driver.execute_script('window.open(arguments[0]);', article_url)
                self.driver.switch_to.window(self.driver.window_handles[1])

                article_soup = bs(self.driver.page_source, 'html.parser')

                category = article_soup.select_one('#container > div.column.col73.mb00 > div:nth-child(1) > div > div.path_wrap > h3').get_text(strip=True)
                title = article_soup.select_one('#container > div.column.col73.mb00 > div:nth-child(1) > div > div.arv_005_01 > div.fix_art_top > div > div > h2').get_text(strip=True)
                full_text = article_soup.select_one(
                        '#container > div.column.col73.mb00 > div:nth-child(1) > div > div.arv_005_01 > div.fix_art_top > div > div > ul.art_info > li:nth-child(2)'
                        ).get_text(strip=True)                
                date_txt = full_text.split()[1]


                sentences = []

                atc = article_soup.select_one('#news_body_area')
                p_tags = atc.find_all('p')

                for p_tag in p_tags:
                    text = p_tag.get_text(strip=True)
                    trimmed = text[13:] if len(text) > 13 else ''
                    sentences.append(trimmed)

                contents = ' '.join(sentences)

                data_set.append({
                    "News" : "시사뉴스",
                    "Date" : date_txt,
                    "Category" : category,
                    "Title" : title,
                    "Contents" : contents
                })


                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])

        self.driver.quit()

        return data_set
    

    def get_ynanews(self, pg_num = 2):
        base_url = "https://www.yna.co.kr/news/{}"
        data_set = []

        max_num = 21
        if pg_num > max_num:
            pg_num = max_num

        for page in range(1, pg_num):
            url = base_url.format(page)
            self.driver.get(url)
            time.sleep(1)

            li_list = self.driver.find_elements(By.CSS_SELECTOR, 'li[data-cid]')
            
            for li in li_list:
                a_tag = li.find_element(By.CSS_SELECTOR, 'a.tit-news')
                article_url = a_tag.get_attribute("href")

                # open new window with article page
                self.driver.execute_script('window.open(arguments[0]);', article_url)
                self.driver.switch_to.window(self.driver.window_handles[1])

                article_soup = bs(self.driver.page_source, 'html.parser')

                genre_tag = article_soup.select_one('meta[itemprop="genre"]')
                category = genre_tag['content'].strip() if genre_tag else ""

                title = article_soup.select_one('#container > div.container591 > div.content90 > header > h1').get_text(strip=True)
                
                date_tag = article_soup.select_one('p.txt-time01')
                if date_tag:
                    date_text = date_tag.get_text(' ', strip=True)
                date_text = date_text.split()[1]
                date_text = date_text.replace("-", ".")

                sentences = []

                atc = article_soup.select_one('#articleWrap > div.story-news.article')
                p_tags = atc.find_all('p')[:-2]

                for p_tag in p_tags:
                    sentences.append(p_tag.get_text(strip = True))

                contents = ' '.join(sentences)

                data_set.append({
                    "News" : "연합뉴스",
                    "Date" : date_text,
                    "Category" : category,
                    "Title" : title,
                    "Contents" : contents
                })


                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])

        self.driver.quit()

        return data_set
    




    def get_donganews(self, pg_num = 2):
        base_url = "https://www.donga.com/news/List?p={}1&prod=news&ymd=&m="

        data_set = []

        if pg_num != 2:
            pg_num = pg_num * 20 + 1

        for page in range(1, pg_num, 20):
            url = base_url.format(page)
            self.driver.get(url)
            time.sleep(1)

            # 기사 제목과 링크 추출
    
            li_list = self.driver.find_elements(By.CSS_SELECTOR, 'ul.row_list > li')
            for li in li_list:
                try:
                    a_tag = li.find_element(By.CSS_SELECTOR, 'h4.tit > a')
                    href = a_tag.get_attribute('href')
                except:
                    continue  # 해당 li는 건너뜀

                # open new window with article page
                self.driver.execute_script('window.open(arguments[0]);', href)
                self.driver.switch_to.window(self.driver.window_handles[1])

                article_soup = bs(self.driver.page_source, 'html.parser')

                element = article_soup.select_one('#contents > header > div > section > nav > ol > li > a')
                if element:
                    category = element.get_text(strip=True)
                else:
                    category = ''

                title = article_soup.select_one('#contents > header > div > section > h1').get_text(strip=True)
                
                date_element = article_soup.select_one('#contents > header > div > section > ul > li:nth-child(2) > button > span:nth-child(1)')
                if date_element:
                    date_text = date_element.get_text(strip=True).split()[0]
                else:
                    date_text = ''
                date_text = date_text.replace("-", ".")

                sentences = []

                content = article_soup.select_one('#contents > div.view_body > div > div.main_view > section.news_view')
                if content:
                    text = content.get_text(separator=' ', strip=True)
                    sentences.append(text)

                contents = ' '.join(sentences)

                data_set.append({
                    "News" : "동아일보",
                    "Date" : date_text,
                    "Category" : category,
                    "Title" : title,
                    "Contents" : contents
                })


                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])

        self.driver.quit()

        return data_set
    







    def get_haninews(self, pg_num = 2):
        base_url = "https://www.hani.co.kr/arti?page={}"

        data_set = []

        for page in range(1, pg_num, 20):
            url = base_url.format(page)
            self.driver.get(url)
            time.sleep(1)

            # 기사 제목과 링크 추출
    
            ul_elem = self.driver.find_element(By.CSS_SELECTOR, 'ul')
            li_list = ul_elem.find_elements(By.TAG_NAME, 'li')
            for li in li_list:
                a_tag = li.find_element(By.CSS_SELECTOR, 'a')
                href = a_tag.get_attribute('href')

                # open new window with article page
                self.driver.execute_script('window.open(arguments[0]);', href)
                self.driver.switch_to.window(self.driver.window_handles[1])

                article_soup = bs(self.driver.page_source, 'html.parser')

                category = article_soup.select_one('#renewal2023 > div.ArticleDetailView_breadcrumb___UwRC > a:nth-child(1)').get_text(strip=True)

                title = article_soup.select_one('#renewal2023 > h3').get_text(strip=True)
                
                date_element = article_soup.select_one('#renewal2023 ul.ArticleDetailView_dateList__tniXJ li span')
                if date_element:
                    date_text = date_element.get_text(strip=True).split()[0]
                else:
                    date_text = ''
                date_text = date_text.replace("-", ".")

                sentences = []

                paragraphs = article_soup.select_one('#renewal2023 > div.article-text > p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text:  # 빈 텍스트는 제외
                        sentences.append(text)

                contents = ' '.join(sentences)

                data_set.append({
                    "News" : "한겨례",
                    "Date" : date_text,
                    "Category" : category,
                    "Title" : title,
                    "Contents" : contents
                })


                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])

        self.driver.quit()

        return data_set