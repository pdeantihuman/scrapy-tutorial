# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import pymysql


class MyPipeline(object):
    def __init__(self, host, db, user, passwd, port):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            host=settings.get('MYSQL_HOST'),
            db=settings.get('MYSQL_DBNAME'),
            user=settings.get('MYSQL_USER'),
            passwd=settings.get('MYSQL_PASSWD'),
            port=settings.getint('MYSQL_PORT')
        )

    def open_spider(self, spider):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                database=self.db,
                user=self.user,
                passwd=self.passwd,
                port=self.port,
                charset="utf8",
                use_unicode=True
            )
        except Exception, e:

            print "******************************************"
            print e
            print "******************************************"
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        query = "INSERT INTO zhihuUser.user (url_token, answer_count,follower_count,name) VALUES ('%s','%s','%s','%s');" % (item['url_token'],item['answer_count'],item['follower_count'],item['name'])
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception, e:
            print '*****************************'
            print(e)
            print '*****************************'
            exit(15)
        return item
