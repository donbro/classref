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


    def exec_sql(self, sql_query, sql_data, print_query=False, print_result=False):
                
        if print_query: print "sql_query is %r" % sql_query
        try:
            cursor = self.conn.cursor()
            cursor.execute( sql_query, sql_data )    
            self.conn.commit()
            r = [z for z in cursor] 
        except self.conn.ProgrammingError as err:
            if err.message == "no results to fetch":
                return None
            print "err is:", err
            raise
        finally:
            cursor.close()    
        if print_result: print "r is", r
        return r

    def get_string_id(self, s, print_query=False, print_result=False):
        # sql_query = "insert into strings (string) values( '" + s + "' )"
        sql_query = "INSERT INTO strings (string) VALUES (%s)"        
        sql_data = [s]
        r = self.exec_sql(sql_query, sql_data, print_query=print_query, print_result=print_result)
        return r[0][0]
 
    def get_name_id(self, name_str_id, supername_str_id):

        # name_ids ( name_str_id, supername_str_id, name_id   )
        
        # sql_query =  "insert into name_ids (name_str_id , supername_str_id) " \
        #                 "values ( '" + name_str_id + "' , '" + supername_str_id + "');"
        sql_query = "INSERT INTO name_ids (name_str_id , supername_str_id) VALUES (%s, %s)"        
        sql_data = [name_str_id , supername_str_id]
        r = self.exec_sql(sql_query, sql_data, print_result=False)
        return r[0][2] # [0]

 
    def process_item(self, item, spider):
        print "process_item(self, %r, %r)" % ( item, spider )
        #return item

        # create relation/triple for (class, method, type) in three steps:

        # (1) get string id for the *label* of each element of our scraped item

        class_name_label_id  = self.get_string_id('class name')
        method_name_label_id = self.get_string_id('method name')
        method_type_label_id = self.get_string_id('method type')

        abstract_label_id    = self.get_string_id('abstract')

        # (2) get string id for the values of each element. if a string exists we will get existing string.

        class_name_str_id   = self.get_string_id( item['class_name'][0]  )
        method_name_str_id  = self.get_string_id( item['method_name'][0] )
        method_type_str_id  = self.get_string_id( item['type']           )  # is a string (not a list like all the others)

        abstract_str_id     = self.get_string_id( item['abstract'][0]    )  

        # (3) create names record for each pair ( id of value of element, id of label of element) 

        class_name_name_id = self.get_name_id(class_name_str_id, class_name_label_id)
        method_name_name_id = self.get_name_id(method_name_str_id, method_name_label_id)
        method_type_name_id = self.get_name_id(method_type_str_id, method_type_label_id)

        abstract_name_id = self.get_name_id(abstract_str_id, abstract_label_id)


        # (4) insert column into relation, ie, insert id for our name record , id for our relatoin) into relations
        # table schema is relations ( name_id , rel_id  )
        
        # (4a) get rel_id for our new relation(s)
        
        sql_query = "select concat( 'rel' , right( concat( '000000' , (1 + (select substring( coalesce( max(rel_id), 'rel00000' ) from 4 for 5) from relations)::integer)::text) , 5) ) ;"
        r = self.exec_sql(sql_query, [])
        print "new relation id is", r

        # (4b) insert into relations

        # sql_query =  "insert into relations (name_id , rel_id) values ( '" + method_type_str_id + "' , '" + method_type_label_id + "');"
        # r = self.exec_sql(sql_query)




        # refs = []
        # for ref in item['seeAlso']:
        #     sql_query = "insert into strings (string) values( '" + ref + "' )"
        #     r = self.exec_sql(sql_query)
        #     ref_str_id = r[0][0]
        #     refs.append(ref_str_id)
        # 
        # 
        return item





import unittest

class untitledTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_010_do_parse_args(self):
        print "C is", C
        
    def test_015_do_parse_args(self):
        sql_query = "select max(str_id) from strings"
        sql_data = []
        C.exec_sql(sql_query, sql_data, print_query=True, print_result=True)
        
    def test_020_do_parse_args(self):
        name_str_id = C.get_string_id('hello wonko!')
        print "name_str_id is", name_str_id
        supername_str_id = C.get_string_id('hello wonko 2!')
        print "supername_str_id is", supername_str_id
        
        name_id = C.get_name_id(name_str_id, supername_str_id)
        print "name_id is %r" % name_id
        
    def test_025_do_parse_args(self):
        
        item = {'abstract': [u"Attempts to modify the application's activation policy."],
         'class_name': [u'NSApplication Class Reference'],
         'method_name': [u'setActivationPolicy:'],
         'seeAlso': [u'\u2013\xa0activationPolicy'],
         'type': 'instanceMethod'}

        class_name_label_id  = C.get_string_id('class name')
        class_name_str_id   = C.get_string_id( item['class_name'][0]  )
        class_name_name_id = C.get_name_id(class_name_str_id, class_name_label_id)


        abstract_label_id    = C.get_string_id('abstract')
        abstract_str_id     = C.get_string_id( item['abstract'][0]    )  
        abstract_name_id = C.get_name_id(abstract_str_id, abstract_label_id)

        # ProgrammingError: syntax error at or near "s"
        # LINE 1: ...ring) values( 'Attempts to modify the application's activati...

if __name__ == '__main__':
    print "hello"
    C = ClassrefPipeline()
    
    unittest.main()
    
    
    
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



    