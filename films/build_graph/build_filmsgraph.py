# coding: utf-8

import os,csv,codecs
import json,time
from py2neo import Graph,Node

class FilmGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.cur_dir = cur_dir
        self.data_path = os.path.join(self.cur_dir, 'film_info.json')
        self.g = Graph(
            host="localhost", 
            http_port=7474, 
            user="neo4j",  
            password="1qaz@WSX")
        
    def get_nodes(self):
        films = []
        baike_url = []
        pic = []
        
        film_infos = []
        chupin_company = []
        faxing_company = []
        areas = []
        directors = []
        scriptwriter = []
        filmmakers = []
        film_types = []
        film_stars = []
        film_release_time = []
        film_languages = []
        
        rels_director = []
        rels_scriptwriter = []
        rels_faxing_company = []
        rels_area = []
        rels_type = []
        rels_star = []
        rels_language = []
        
        count = 0
        with codecs.open(self.data_path,"r","utf-8") as f:
            for data in f:
                film_dict = {}
                count += 1
                print(count)
                data_json = json.loads(data)
                film = data_json['film_name']
                film_dict['name'] = film
                films.append(film)
                film_dict['chinese_name'] = ''
                film_dict['film_baike_id'] = ''
                film_dict['pic_url'] = ''
                film_dict['film_baike_url'] = ''
                film_dict['foreign_name'] = ''
                film_dict['other_name'] = ''
                film_dict['film_classification'] = ''
                
                if 'chinese_name' in data_json:
                    film_dict['chinese_name'] = " ".join(data_json['chinese_name'])
                    
                if 'film_baike_id' in data_json:
                    film_dict['film_baike_id'] = data_json['film_baike_id']
                if "pic" in data_json:
                    film_dict["pic_url"] = data_json["pic"]
                    
                if "film_baike_url" in data_json:
                    film_dict['film_baike_url'] = data_json["film_baike_url"]
                    
                if "foreign_name" in data_json:
                    film_dict["foreign_name"] = " ".join(data_json["foreign_name"])
                
                if "other_name" in data_json:
                    film_dict["other_name"] = " ".join(data_json["other_name"])
                    
                if "film_classification" in data_json:
                    film_dict["film_classification"] = " ".join(data_json['film_classification'])
                    
                if 'director' in data_json:
                    directors += data_json['director']
                    for director in data_json['director']:
                        if director and data_json['film_baike_id']:
                            rels_director.append([film, director])
    
                if 'scriptwriter' in data_json:
                    for scriptwriter in data_json['scriptwriter']:
                        if scriptwriter and data_json['film_baike_id']:
                            rels_scriptwriter.append([film, scriptwriter])
                        
                if "faxing_company" in data_json:
                    faxing_company += data_json["faxing_company"]
                    for company in data_json["faxing_company"]:
                        if company and data_json['film_baike_id']:
                            rels_faxing_company.append([film,company])
                        
                if "producer area" in data_json:
                    areas += data_json["producer area"]
                    for area in data_json["producer area"]:
                        if area and data_json['film_baike_id']:
                            rels_area.append([film,area])
                        
                if "film_type" in data_json:
                    film_types += data_json["film_type"]
                    for film_type in data_json["film_type"]:
                        if film_type and data_json['film_baike_id']:
                            rels_type.append([film,film_type])
                        
                if "film_star" in data_json:
                    film_stars += data_json["film_star"]
                    for film_star in data_json["film_star"]:
                        if film_star and data_json['film_baike_id']:
                            rels_star.append([film,film_star])
                        
                if "film_language" in data_json:
                    film_languages += data_json["film_language"]
                    for language in data_json["film_language"]:
                        if language and data_json['film_baike_id']:
                            rels_language.append([film,language])
                film_infos.append(film_dict)
        return set(films),set(directors),set(film_stars),set(film_types),set(faxing_company),set(areas),set(film_languages),\
               film_infos,rels_director,rels_star,rels_type,rels_faxing_company,rels_area,rels_language,rels_scriptwriter
    def create_films_node(self,films_infos):
        count = 0
        for film_info in films_infos:
            node = Node("Film", name=film_info['chinese_name'], baike_id=film_info['film_baike_id'],
                        pic_url=film_info['pic_url'] ,baike_url=film_info['film_baike_url'],
                        foreign_name=film_info['foreign_name'],other_name=film_info['other_name'],
                        classification=film_info['film_classification'])
            self.g.create(node)
            count += 1
            print(count)
        return


    def write_dicts2csv(self,csv_file_path):
        '''
        把数据字典列表写入csv文件
        :param dicts: 数据字典列表
        :param csv_file_path: 要写入的csv文件路径
        '''
        films,director,stars,types,company,areas,languages,film_infos,rels_director,rels_star,rels_type,\
            rels_faxing_company,rels_area,rels_language,rels_scriptwriter = handler.get_nodes()        
        
        with codecs.open(csv_file_path,'wb+',encoding="utf-8") as csv_file:
            # 获取表头列名列表
            headers = film_infos[0].keys()
            writer = csv.DictWriter(csv_file,fieldnames = headers)
            # 写入表头
            writer.writeheader()
            # 写入数据行
            writer.writerows(film_infos)
    def write_list2csv(self,data_list,csv_file_path):
        with codecs.open(csv_file_path,'wb+',encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["name"])
            for row in data_list:
                writer.writerow([row]) #[row] to avoid ","

    def write_rels_list2csv(self,data_list,csv_file_path):
        with open(csv_file_path,'w',newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["from_name","to_name"])
            for row in data_list:
                writer.writerow(row) #[row] to avoid ","
    def create_rels_from_csv(self,csv_file_name,start_node,end_node,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 match (from:%s{baike_id:line.from_name}),(to:%s{name:line.to_name}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,start_node,end_node,rel_type,rel_name)
        try:
            self.g.run(query)
            print("Create relationship successfully")
        except Exception as e:
            print(e)        
    def create_node(self,csv_name):
        query = "USING PERIODIC COMMIT 10000 \
                LOAD CSV WITH HEADERS FROM 'file:///types.csv' AS row \
                CREATE (:Film_Type {name: row.name})"    
        try:
            self.g.run(query)
            print("Create Node successfully")
        except Exception as e:
            print(e)   
    def nodeExist(self, baike_id):
        matcher = NodeMatcher(self.g)
        m = matcher.match("Baike", baike_id = baike_id).first()
        if m is None:
            return False
        else:
            return True
    def create_graphrels(self):
        films,directors,stars,types,company,areas,languages,film_infos,rels_director,rels_star,rels_type,\
            rels_faxing_company,rels_area,rels_language,rels_scriptwriter = handler.get_nodes()        
        #self.create_relationship('Film', 'Film_Director', rels_director, 'director', '导演')
        self.create_relationship('Film', 'Film_Star', rels_star, 'actor', '主演')
        self.create_relationship('Film', 'Film_Type', rels_type, 'type', '类型')
        self.create_relationship('Film', 'Film_Company', rels_faxing_company, 'faxing_company', '发行公司')
        self.create_relationship('Film', 'Film_Area', rels_area, 'area', '地区')
        self.create_relationship('Film', 'Film_Language', rels_language, 'language', '语言')

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        count = 0
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' merge (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
                continue
        return

if __name__ == '__main__':
    import subprocess
    handler = FilmGraph()
    handler.create_graphrels()
    films,directors,stars,types,company,areas,languages,film_infos,rels_director,rels_star,rels_type,\
        rels_faxing_company,rels_area,rels_language,rels_scriptwriter = handler.get_nodes()
    #handler.create_films_node(film_infos)
    #write2csv = [films,directors,stars,types,company,areas,languages]
    #csv_name = ["films","directors","stars","types","company","areas","languages"]
    #if not os.path.exists(handler.cur_dir+"/films_data"):
        #os.mkdir(handler.cur_dir+"/films_data")
    #for i,d in enumerate(csv_name):
        #handler.create_node(csv_name[i]+".csv")
        #handler.write_list2csv(d,"films_data/%s.csv"%(csv_name[i]))
    info = {"rels_director":{"data":rels_director,"csv_name":"rels_director.csv","start_node":"Film","end_node":"Film_Director","rel_type":"director","rel_name":"导演"},\
            "rels_star":{"data":rels_star,"csv_name":"rels_star.csv","start_node":"Film","end_node":"Film_Star","rel_type":"actor","rel_name":"主演"},
            "rels_type":{"data":rels_type,"csv_name":"rels_type.csv","start_node":"Film","end_node":"Film_Type","rel_type":"type","rel_name":"类型"},
            "rels_faxing_company":{"data":rels_faxing_company,"csv_name":"rels_faxing_company.csv","start_node":"Film","end_node":"Film_Company","rel_type":"faxing_company","rel_name":"发行公司"},
            "rels_area":{"data":rels_area,"csv_name":"rels_area.csv","start_node":"Film","end_node":"Film_Area","rel_type":"area","rel_name":"地区"},
            "rels_language":{"data":rels_language,"csv_name":"rels_language.csv","start_node":"Film","end_node":"Film_Language","rel_type":"language","rel_name":"语言"}}
    for k,v in info.items():
        data,csv_name,start_node,end_node,rel_type,rel_name = v["data"],v["csv_name"],v["start_node"],v["end_node"],v["rel_type"],v["rel_name"]
        #handler.write_rels_list2csv(data,csv_name)
        path = "/opt/knowledgeGraph/films/rels_film"
        cmd="docker cp %s 85b5d7fc2f1c:/var/lib/neo4j/import"%(os.path.join(path,csv_name))
        try:
            subprocess.call(cmd, shell=True)
        except Exception as error:
            print(error)    
        handler.create_rels_from_csv(csv_file_name=csv_name, start_node=start_node, end_node=end_node, 
                                    rel_type=rel_type, 
                                    rel_name=rel_name)
        print("Done of {}".format(k))



