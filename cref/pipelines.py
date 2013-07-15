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

    def process_item(self, item, spider):
        print "process_item(self, %r, %r)" % ( item, spider )
        #return item

        # get rel_id for our new relation(s)
        
        sql = "select concat( 'rel' , right( concat( '000000' , (1 + (select substring( coalesce( max(rel_id), 'rel00000' ) from 4 for 5) from relations)::integer)::text) , 5) ) ;"
        r = self.exec_sql(sql)
        print "new relation id is", r

        # get string ids for the *labels* of each element of our item

        sql_query = "insert into strings (string) values( '" + 'class name' + "' )"
        r = self.exec_sql(sql_query)
        class_name_label_id = r[0][0]

        sql_query = "insert into strings (string) values( '" + 'method_name' + "' )"
        r = self.exec_sql(sql_query)
        method_name_label_id = r[0][0]

        sql_query = "insert into strings (string) values( '" + 'type' + "' )"
        r = self.exec_sql(sql_query)
        method_type_label_id = r[0][0]

        sql_query = "insert into strings (string) values( '" + 'abstract' + "' )"
        r = self.exec_sql(sql_query)
        abstract_label_id = r[0][0]


        # get string ids for each string, if a string exists we will get existing string

        sql_query = "insert into strings (string) values( '" + item['class_name'][0] + "' )"
        r = self.exec_sql(sql_query)
        class_name_str_id = r[0][0]

        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + class_name_str_id + "' , '" + class_name_label_id + "');"
        r = self.exec_sql(sql_query)


        sql_query = "insert into strings (string) values( '" + item['method_name'][0] + "' )"
        r = self.exec_sql(sql_query)
        method_name_str_id = r[0][0]

        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + method_name_str_id + "' , '" + method_name_label_id + "');"
        r = self.exec_sql(sql_query)

        sql_query = "insert into strings (string) values( '" + item['type'][0] + "' )"
        r = self.exec_sql(sql_query)
        method_type_str_id = r[0][0]

        sql_query =  "insert into names_ids (string_id , super_name_id) values ( '" + method_type_str_id + "' , '" + method_type_label_id + "');"
        r = self.exec_sql(sql_query)


        sql_query = "insert into strings (string) values( '" + item['abstract'][0] + "' )"
        r = self.exec_sql(sql_query)
        abstract_str_id = r[0][0]

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

    def exec_sql(self, sql_query):
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

