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
            command_executor="http://remote_chromedriver:4444/wd/hub",
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