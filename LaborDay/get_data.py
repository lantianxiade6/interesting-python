import requests
import pandas as pd
from pymongo import MongoClient


class DataCrawler(object):
    def __init__(self):
        self.cities = list(pd.read_csv('./LaborDay/city_data2.csv')['city'])#全国34个省市列表，取出市 #注意改路径
        client = MongoClient(host='localhost', port=27017)#连接mongodb
        db = client.Laborday#新建数据库Laborday
        self.col = db.ticket#新建表

    def get_city_trip(self):
        for city in self.cities:#遍历每一个城市
            print('正在爬取城市:{}的数据!'.format(city))#打印当前开始爬取的城市
            #打开飞猪主页，搜索“广州”，点开第2页，就能在开发者工具-Network-某请求的Headers里面找到类似的
            res = requests.get('https://travelsearch.fliggy.com/async/queryItemResult.do?searchType='
                               'product&keyword={}&category=SCENIC&pagenum=1'.format(city))#只爬一页
            data = res.json()#转为json
            itemPagenum = data['data']['data'].get('itemPagenum')
            if itemPagenum is not None:
                page_count = itemPagenum['data']['count']#获取一共有多少页

                data_list = data['data']['data']['itemProducts']['data']['list'][0]['auctions']#获取产品列表数据所在之处
                for ticket in data_list:#遍历每个产品
                    ticket['city'] = city#在原数据中增加1个'city'字段，内容就是city
                    self.col.insert_one(ticket)#插入该数据到ticket表中去
                print('成功爬取城市:{}的第{}页数据!'.format(city, 1))#打印当前已完成的页数

                if page_count > 1:#如果该城市不只一页
                    for page in range(2, page_count+1):#遍历第2页到最后一页
                        res = requests.get('https://travelsearch.fliggy.com/async/queryItemResult.do?searchType='
                                           'product&keyword={}&category=SCENIC&pagenum={}'.format(city, page))#爬取第page页
                        data = res.json()#转为json
                        if data['data']['data']['itemProducts'] is not None:
                            data_list = data['data']['data']['itemProducts']['data']['list'][0]['auctions']#获取产品列表数据所在之处
                            for ticket in data_list:#遍历每个产品
                                ticket['city'] = city#在原数据中增加1个'city'字段，内容就是city
                                self.col.insert_one(ticket)#插入该数据到ticket表中去
                            print('成功爬取城市:{}的第{}页数据!'.format(city, page))#打印当前已完成的页数


if __name__ == '__main__':
    data_crawler = DataCrawler()#类DataCrawler的一个实例
    data_crawler.get_city_trip()#调用get_city_trip方法进行爬数