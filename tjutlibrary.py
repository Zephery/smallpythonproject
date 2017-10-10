import logging
import multiprocessing
import socket
import time
import urllib.parse
import urllib.request

import pymysql
from bs4 import BeautifulSoup

# mysql
conn = pymysql.connect(host='119.23.46.71', user='root', passwd='root', db='ligong', port=3306)
cur = conn.cursor()

#socket
socket.setdefaulttimeout(10)

#  logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='log/test.log',
                    filemode='a')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

insert_sql = """insert into ligong.book2(stu_id,stu_name,bookname,writer,rentdate,returndate,index_id)
VALUES (%s,%s,%s,%s,%s,%s,%s)"""

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1; '
                  '360Spider(compatible; HaosouSpider'}  # 构造头部


def get_book(stu_id):
    try:
        # url
        url = "http://211.81.31.34/uhtbin/cgisirsi/x/0/0/57/49?user_id=LIBSCI_ENGI&password=LIBSC"
        res = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(res, "html.parser")
        login_url = "http://211.81.31.34" + urllib.parse.quote(soup.findAll("form")[1]['action'])

        params = dict(
            user_id="R0500A" + str(stu_id),
            password="000"
        )
        data = urllib.parse.urlencode(params).encode("utf-8")
        req = urllib.request.Request(login_url, data=data, headers=headers)
        with urllib.request.urlopen(req) as f:
            login_page = BeautifulSoup(f.read(), 'html.parser')
            stu_name = login_page.find("div", class_='login_container').find('p').getText().split("|")[0].strip()
            print(stu_name)
            my_count = login_page.find_all('a', class_='rootbar')[1]['href']
            my_count = "http://211.81.31.34" + urllib.parse.quote(my_count)
            with urllib.request.urlopen(my_count) as ff:
                href = BeautifulSoup(ff.read(), "html.parser").find_all('ul', class_='gatelist_table')[0].find('a')[
                    'href']
                a_href = "http://211.81.31.34" + urllib.parse.quote(href)
                with urllib.request.urlopen(a_href) as fff:
                    content = BeautifulSoup(fff.read(), "html.parser")
                    try:
                        rent = content.find("tbody", id="tblCharge").find_all('tr')
                        for i in rent:
                            bookname = i.find(class_="accountstyle").getText().strip()
                            author = i.find(class_="accountstyle author", align="left").getText().strip()
                            date_return = i.find(class_="accountstyle due_date").getText().strip()
                            cur.execute(insert_sql, (stu_id, stu_name, bookname, author, None, date_return, None))
                            conn.commit()
                    except:
                        cur.execute(insert_sql, (stu_id, stu_name, None, None, None, None, None))
                        conn.commit()
                        logging.info(str(stu_id) + "当前没有借阅书籍")
                    history = content.find("tbody", id="tblSuspensions").find_all('tr')
                    for i in history:
                        bookname = i.find(class_="accountstyle").getText().strip()
                        author = i.find(class_="accountstyle author", align="left").getText().strip()
                        Date_Charged = i.find(class_="accountstyle due_date", align="center").getText().strip()
                        Date_Returned = i.find(class_="accountstyle due_date", align="left").getText().strip()
                        bookindex = i.find(class_="accountstyle author", align="center").getText().strip()
                        cur.execute(insert_sql,
                                    (stu_id, stu_name, bookname, author, Date_Charged, Date_Returned, bookindex))
                        conn.commit()
    except urllib.request.URLError as e:
        logging.error(e)
        logging.info(str(stu_id) + "reconnecting")
        time.sleep(4)
        get_book(stu_id)
    except socket.timeout as e:
        logging.error(e)
        logging.info("timeout"+str(stu_id) + "reconnecting")
        time.sleep(4)
        get_book(stu_id)
    except Exception as e:
        logging.error(e)
        logging.info(stu_id)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=4)
    for i in range(20130001, 20137100):
        if str(i)[4] >= '6':
            continue
        else:
            # get_book(i)
            pool.apply_async(get_book, (i,))
    pool.close()
    pool.join()
    cur.close()
    conn.close()
