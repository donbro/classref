#!/Users/donb/projects/VENV/scrapy/bin/python
# encoding: utf-8

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html


import psycopg2

class ClassrefPipeline(object):

    def __init__(self):
        try:
            self.conn = psycopg2.connect("dbname='files' user='donb' host='localhost' password=''")
            print "Connect to database succeeded."
        except:
            print "I am unable to connect to the database"

    # each item is a relation, possibly more

        # 'abstract': [u'Returns the application instance, creating it if it doesn\u2019t exist yet.'],
        #  'class_name': [u'NSApplication Class Reference'],
        #  'method_name': [u'sharedApplication'],
        #  'seeAlso': [u'\u2013\xa0run', u'\u2013\xa0terminate:'],
        #  'type': 'classMethod'


    def exec_sql(self, sql_query, print_query=True, print_result=True):
        print "sql_query is %r" % sql_query
        try:
            cursor = self.conn.cursor()
            cursor.execute( sql_query )    
            self.conn.commit()
            r = [z for z in cursor] 
        except self.conn.ProgrammingError as err:
            if err.message == "no results to fetch":
                return None
        finally:
            cursor.close()    
        print "r is", r
        return r

    def get_string_id(self, s, print_query=True, print_result=True):
        sql_query = "insert into strings (string) values( '" + s + "' )"
        r = self.exec_sql(sql_query, print_query=True, print_result=True)
        return r[0][0]
 
    def get_names_id(self, name_str_id, superclass_str_id):

        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + name_str_id + "' , '" + superclass_str_id + "');"
        r = self.exec_sql(sql_query)
        sql_query =  "select from names_ids where string_id = '" +  name_str_id + "' and  super_name_id = '" + superclass_str_id +  "');"
        r = self.exec_sql(sql_query)
        return r[0][0]

 
    def process_item(self, item, spider):
        print "process_item(self, %r, %r)" % ( item, spider )
        #return item

        # get rel_id for our new relation(s)
        
        sql = "select concat( 'rel' , right( concat( '000000' , (1 + (select substring( coalesce( max(rel_id), 'rel00000' ) from 4 for 5) from relations)::integer)::text) , 5) ) ;"
        r = self.exec_sql(sql)
        print "new relation id is", r

        # create relation/triple for (class, method, type) in three steps:

        # (1) get string id for the *label* of each element of our scraped item

        class_name_label_id  = self.get_string_id('class name')
        method_name_label_id = self.get_string_id('method name')
        method_type_label_id = self.get_string_id('method type')

        abstract_label_id = self.get_string_id('abstract')

        # (2) get string id for the values of each element. if a string exists we will get existing string.

        class_name_str_id = self.get_string_id( item['class_name'][0] )
        method_name_str_id = self.get_string_id( item['method_name'][0] )
        method_type_str_id = self.get_string_id( item['type'] )  # this element is a string (not a list like all the others)

        abstract_str_id = self.get_string_id( item['abstract'][0] )  

        # (3) create names record for each pair (value, label_name) 

        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + class_name_str_id + "' , '" + class_name_label_id + "');"
        r = self.exec_sql(sql_query)

        # insert column into relation, ie, insert id for our name record , id for our relatoin) into relations
        # table schema is relations ( name_id , rel_id  )

        sql_query =  "insert into relations (name_id , rel_id) values ( '" + method_type_str_id + "' , '" + method_type_label_id + "');"
        r = self.exec_sql(sql_query)


        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + method_name_str_id + "' , '" + method_name_label_id + "');"
        r = self.exec_sql(sql_query)

        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + method_type_str_id + "' , '" + method_type_label_id + "');"
        r = self.exec_sql(sql_query)


        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + abstract_str_id + "' , '" + abstract_label_id + "');"
        r = self.exec_sql(sql_query)


        refs = []
        for ref in item['seeAlso']:
            sql_query = "insert into strings (string) values( '" + ref + "' )"
            r = self.exec_sql(sql_query)
            ref_str_id = r[0][0]
            refs.append(ref_str_id)

        #print abstract_str_id, class_name_str_id, self.exec_sql, see_also_str_id, method_type_str_id

        #sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + name_str_id + "' , '" + type_str_id + "');"
        #r = self.exec_sql(sql_query)

        return item



#    def process_item(self, item, spider):
#                
#                cur = self.conn.cursor()
#                cur.execute("SELECT id FROM paco_app_conference WHERE name LIKE %s", (item['name'],))
#                if len(cur.fetchall()) == 0:
#                        cur.execute("INSERT INTO paco_app_conference (name, description, date_added, type, location, url) VALUES (%s, %s, now(), 'C', %s, %s)", (item['name'], item['desc'], item['location'], item['link']))
#                        self.conn.commit()
#                        
#                        cur.execute("SELECT max(id) FROM paco_app_conference")
#                        id_conf = cur.fetchall()[0][0]
#                        
#                        cur.execute("INSERT INTO paco_app_date (name, deadline, date_added) VALUES ('Start-Date', %s, now())", (item['startdate'],))
#                        self.conn.commit()
#                        
#                        cur.execute("SELECT max(id) FROM paco_app_date")
#                        id_date = cur.fetchall()[0][0]
#                        
#                        cur.execute("INSERT INTO paco_app_conference_milestones (conference_id, date_id) VALUES (%s, %s)", (id_conf, id_date))
#                        
#                        cur.execute("INSERT INTO paco_app_date (name, deadline, date_added) VALUES ('End-Date', %s, now())", (item['enddate'],))
#                        self.conn.commit()
#                        
#                        cur.execute("SELECT max(id) FROM paco_app_date")
#                        id_date = cur.fetchall()[0][0]
#                        
#                        cur.execute("INSERT INTO paco_app_conference_milestones (conference_id, date_id) VALUES (%s, %s)", (id_conf, id_date))
#                        self.conn.commit()
#                        return item
#                else:
#                        print '[DOUBLE]'





import unittest

class untitledTests(unittest.TestCase):
    def setUp(self):
        print "hello"
        self.C = ClassrefPipeline()
        pass

    def test_010_do_parse_args(self):
        print "self.C is", self.C
        
    def test_015_do_parse_args(self):
        sql_query = "select max(str_id) from strings"
        self.C.exec_sql(sql_query, print_query=True, print_result=True)
        
    def test_020_do_parse_args(self):
        str_id = self.C.get_string_id('hello wonko!')
        print "str_id is", str_id


if __name__ == '__main__':
    unittest.main()