import os
import re
import time
import requests
from pymongo import MongoClient
from info import rent_type, city_info#调用来自info.py的字典


class Rent(object):
    """
    初始化函数，获取租房类型（整租、合租）、要爬取的城市分区信息以及连接mongodb数据库
    """
    def __init__(self):
        self.rent_type = rent_type#传入rent_type和city_info字典
        self.city_info = city_info

        host = os.environ.get('MONGODB_HOST', '127.0.0.1')  # 本地数据库
        port = os.environ.get('MONGODB_PORT', '27017')  # 数据库端口
        mongo_url = 'mongodb://{}:{}'.format(host, port)#数据库地址
        mongo_db = os.environ.get('MONGODB_DATABASE', 'Lianjia')#？
        client = MongoClient(mongo_url)#连接本地的MongoDB
        self.db = client[mongo_db]#创建一个叫Lianjia的数据库
        self.db['zufang'].create_index('m_url', unique=True)  # 创建一个叫zufang的集合，以m_url为主键进行去重

    def get_data(self):
        """
        爬取不同租房类型、不同城市各区域的租房信息
        :return: None
        """
        for ty, type_code in self.rent_type.items():  # 整租、合租 #遍历整租和合租（ty是key，type_code是值）
            for city, info in self.city_info.items():  # 城市、城市各区的信息 ##遍历各城市（city是key，info是值）
                for dist, dist_py in info[2].items():  # 各区及其拼音 #遍历各区（dist是key，dist_py是值）
                    #广州天河区租房连接：https://m.lianjia.com/chuzu/gz/zufang/tianhe/
                    #https://gz.lianjia.com/zufang/tianhe/
                    res_bc = requests.get('https://m.lianjia.com/chuzu/{}/zufang/{}/'.format(info[1], dist_py))#打开某市某区（手机版链接）
                    pa_bc = r"data-type=\"bizcircle\" data-key=\"(.*)\" class=\"oneline \">"
                    bc_list = re.findall(pa_bc, res_bc.text)#找到所有商圈
                    self._write_bc(bc_list)#导出bc_list(见最底的函数)
                    bc_list = self._read_bc()  #导入bc_list(见最底的函数)# 先爬取各区的商圈，最终以各区商圈来爬数据，如果按区爬，每区最多只能获得2000条数据

                    if len(bc_list) > 0:
                        for bc_name in bc_list:#遍历每一个商圈
                            idx = 0
                            has_more = 1
                            while has_more:
                                try:
                                    #要输入城市码，商圈名字，整租或合租码，每30一页，时间戳(单位秒)
                                    url = 'https://app.api.lianjia.com/Rentplat/v1/house/list?city_id={}&condition={}' \
                                          '/rt{}&limit=30&offset={}&request_ts={}&scene=list'.format(info[0],
                                                                                                     bc_name,
                                                                                                     type_code,
                                                                                                     idx*30,
                                                                                                     int(time.time()))
                                    #类似：https://app.api.lianjia.com/Rentplat/v1/house/list?city_id=440100&condition=huanghuagangrt200600000001&limit=30&offset=0
                                    res = requests.get(url=url, timeout=10)#发送请求，超时10秒
                                    print('成功爬取{}市{}-{}的{}第{}页数据！'.format(city, dist, bc_name, ty, idx+1))#市-区-商圈-整租/合租-页数
                                    item = {'city': city, 'type': ty, 'dist': dist}
                                    self._parse_record(res.json()['data']['list'], item)#取出数据(见下方的函数)

                                    total = res.json()['data']['total']
                                    idx += 1
                                    if total/30 <= idx:
                                        has_more = 0
                                    # time.sleep(random.random())
                                except:
                                    print('链接访问不成功，正在重试！')

    def _parse_record(self, data, item):
        """
        解析函数，用于解析爬回来的response的json数据
        :param data: 一个包含房源数据的列表
        :param item: 传递字典
        :return: None
        """
        if len(data) > 0:
            for rec in data:
                item['bedroom_num'] = rec.get('frame_bedroom_num')#房数
                item['hall_num'] = rec.get('frame_hall_num')#厅数
                item['bathroom_num'] = rec.get('frame_bathroom_num')#卫生间数
                item['rent_area'] = rec.get('rent_area')#面积
                item['house_title'] = rec.get('house_title')#title
                item['resblock_name'] = rec.get('resblock_name')#小区名字
                item['bizcircle_name'] = rec.get('bizcircle_name')#区域
                item['layout'] = rec.get('layout')#几室几厅几卫
                item['rent_price_listing'] = rec.get('rent_price_listing')#价格
                item['house_tag'] = self._parse_house_tags(rec.get('house_tags'))#取出tag,见下方函数
                item['frame_orientation'] = rec.get('frame_orientation')#房子朝向
                item['m_url'] = rec.get('m_url')#路径
                item['rent_price_unit'] = rec.get('rent_price_unit')#元/月

                try:
                    res2 = requests.get(item['m_url'], timeout=5)
                    pa_lon = r"longitude: '(.*)',"
                    pa_lat = r"latitude: '(.*)'"
                    pa_distance = r"<span class=\"fr\">(\d*)米</span>"
                    item['longitude'] = re.findall(pa_lon, res2.text)[0]#取出经度
                    item['latitude'] = re.findall(pa_lat, res2.text)[0]#取出维度
                    distance = re.findall(pa_distance, res2.text)#取出距离
                    if len(distance) > 0:
                        item['distance'] = distance[0]
                    else:
                        item['distance'] = None
                except:
                    item['longitude'] = None
                    item['latitude'] = None
                    item['distance'] = None

                self.db['zufang'].update_one({'m_url': item['m_url']}, {'$set': item}, upsert=True)#插入数据到数据库
                print('成功保存数据:{}!'.format(item))

    @staticmethod
    def _parse_house_tags(house_tag):
        """
        处理house_tags字段，相当于数据清洗
        :param house_tag: house_tags字段的数据
        :return: 处理后的house_tags
        """
        if len(house_tag) > 0:
            st = ''
            for tag in house_tag:
                st += tag.get('name') + ' '
            return st.strip()

    @staticmethod
    def _write_bc(bc_list):
        """
        把爬取的商圈写入txt，为了整个爬取过程更加可控
        :param bc_list: 商圈list
        :return: None
        """
        with open('./BSGS_Rent/bc_list.txt', 'w') as f:
            for bc in bc_list:
                f.write(bc+'\n')

    @staticmethod
    def _read_bc():
        """
        读入商圈
        :return: None
        """
        with open('./BSGS_Rent/bc_list.txt', 'r') as f:
            return [bc.strip() for bc in f.readlines()]


if __name__ == '__main__':
    rent = Rent()#实例化一个Rent类
    rent.get_data()#调用get_data()方法
