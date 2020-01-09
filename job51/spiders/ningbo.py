import re

import scrapy
import pymongo

from scrapy_redis.spiders import RedisSpider

from job51.items import Job51CompanyIndustry
from job51.settings import MONGO_DB, MONGO_HOST, MONGO_PORT


class Ningbo(RedisSpider):
    name = 'ningbo'

    redis_key = 'job51_ningbo:urls'

    category_list = [
        {
            'c': '计算机/互联网/通信/电子',
            'category': {
                "01": "计算机软件",
                "37": "计算机硬件",
                "38": "计算机服务(系统、数据服务、维修)",
                "31": "通信/电信/网络设备",
                "39": "通信/电信运营、增值服务",
                "32": "互联网/电子商务",
                "40": "网络游戏",
                "02": "电子技术/半导体/集成电路",
                "35": "仪器仪表/工业自动化"
            }
        },
        {
            'c': '会计/金融/银行/保险',
            'category': {
                "41": "会计/审计",
                "03": "金融/投资/证券",
                "42": "银行",
                "43": "保险",
                "62": "信托/担保/拍卖/典当"
            }
        },
        {
            'c': '贸易/消费/制造/营运',
            'category':{
                "04": "贸易/进出口",
                "22": "批发/零售",
                "05": "快速消费品(食品、饮料、化妆品)",
                "06": "服装/纺织/皮革",
                "44": "家具/家电/玩具/礼品",
                "60": "奢侈品/收藏品/工艺品/珠宝",
                "45": "办公用品及设备",
                "14": "机械/设备/重工",
                "33": "汽车及零配件"
                    }
        },
        {
            'c': '制药/医疗',
            'category': {
                "08": "制药/生物工程",
                "46": "医疗/护理/卫生",
                "47": "医疗设备/器械"
            }
        },
        {
            'c': '广告/媒体',
            'category': {
                "12": "广告",
                "48": "公关/市场推广/会展",
                "49": "影视/媒体/艺术/文化传播",
                "13": "文字媒体/出版",
                "15": "印刷/包装/造纸"
            }
        },
        {
            'c': '房地产/建筑',
            'category': {
                "26": "房地产",
                "09": "建筑/建材/工程",
                "50": "家居/室内设计/装潢",
                "51": "物业管理/商业中心"
            }
        },
        {
            'c': '专业服务/教育/培训',
            'category': {
                "34": "中介服务",
                "63": "租赁服务",
                "07": "专业服务(咨询、人力资源、财会)",
                "59": "外包服务",
                "52": "检测，认证",
                "18": "法律",
                "23": "教育/培训/院校",
                "24": "学术/科研"
            }
        },
        {
            'c': '服务业',
            'category': {
                "11": "餐饮业",
                "53": "酒店/旅游",
                "17": "娱乐/休闲/体育",
                "54": "美容/保健",
                "27": "生活服务"
            }
        },
        {
            'c': '物流/运输',
            'category': {
                "21": "交通/运输/物流",
                "55": "航天/航空"
            }
        },
        {
            'c': '能源/原材料',
            'category': {
                "19": "石油/化工/矿产/地质",
                "16": "采掘业/冶炼",
                "36": "电气/电力/水利",
                "61": "新能源",
                "56": "原材料和加工"
            }
        },
        {
            'c': '政府/非营利组织/其他',
            'category': {
                "28": "政府/公共事业",
                "57": "非营利组织",
                "20": "环保",
                "29": "农/林/牧/渔",
                "58": "多元化业务集团公司"
            }
        }
    ]
    tag = 'ningbo'
    city = '宁波'

    mongo_db = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)[MONGO_DB]

    def parse(self, response):
        self.logger.debug('正在爬取第{}页({})'.format(response.meta.get('page', 1), response.request.url))
        links = response.xpath('//div[@class="dw_table"]/div[@class="el"]')
        for link in links:
            item = Job51CompanyIndustry()
            item['tag'] = self.tag
            item['city'] = self.city
            company_name = link.xpath('./span/a/@title').get()
            item['url'] = link.xpath('./span/a/@href').get()
            item['company_name'] = company_name
            item['release_time'] = link.xpath('./span[@class="t5"]/text()').get()
            item['district_name'] = link.xpath('./span[@class="t3"]/text()').get()
            remote_item = self.mongo_db[Job51CompanyIndustry.__name__].find_one({'url': item['url']})
            if remote_item is not None:
                self.logger.debug('该公司({})已有爬取记录'.format(company_name))
                continue
            yield scrapy.Request(
                url=item['url'],
                dont_filter=True,
                meta={'item': item},
                callback=self.parse_detail
            )
        
        if not links:
            self.logger.debug('出现异常({})'.format(response.url))
            yield response.request
            return
    
        if response.meta.get('is_first', True):
            total_page = response.css('#hidTotalPage::attr(value)').get()
            if total_page:
                self.logger.debug('({})总页数为{}'.format(response.request.url, total_page))
                total_page = int(total_page)
                if total_page >= 2:
                    for page in range(2, total_page+1):
                        url = re.sub(r',(\d+)\.html?', ',{}.html'.format(page), response.request.url, 1)
                        yield scrapy.Request(
                            url=url,
                            dont_filter=True,
                            meta={'page': page, 'is_first': False}
                        )
    

    def parse_detail(self, response):
        self.logger.debug('正在爬取({})详细页信息'.format(response.request.url))
        item = response.meta['item']
        industry = response.xpath('//p[@class="ltype"]/a/text()').getall()
        map_data = response.xpath('//a[@class="icon_b i_map"]/@onclick').get()
        map_url = None
        address = None
        if map_data:
            address = re.search(r'\'(.*?)\'\);', map_data)
            if address:
                address = address.group(1)
            map_url = re.search(r'\(\'(.*?)\',', map_data)
            if map_url:
                map_url = map_url.group(1)
        category = []
        category_industry_map = {}
        for cate_name in category:
            c = self.find_category(cate_name)
            if c:
                category.append(c)
                if c not in category_industry_map:
                    category_industry_map[c] = []
                category_industry_map[c].append(cate_name)
        item['industry'] = industry
        item['address'] = address
        item['category'] = category
        item['category_industry_map'] = category_industry_map
        if map_url:
            yield scrapy.Request(
                url=map_url,
                dont_filter=True,
                meta={'item': item},
                callback=self.parse_map
            )
        else:
            self.logger.debug('获取详细页中的地图链接失败({})'.format(response.request.url))
            self.logger.debug(dict(item))
        
    def parse_map(self, response):
        self.logger.debug('正在爬取({})地图数据'.format(response.request.url))
        longitude = re.search(r'lng:"(.*?)"', response.text)    
        if longitude:
            longitude = longitude.group(1)
        latitude = re.search(r'lat:"(.*?)"', response.text)
        if latitude:
            latitude  = latitude.group(1)
        item = response.meta['item']
        item['longitude'] = longitude
        item['latitude'] = latitude
        yield item


    def find_category(self, name):
        for cate in self.category_list:
            if name in list(cate['category'].values()):
                return cate['c']
