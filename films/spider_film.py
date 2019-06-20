import requests,json,codecs,re,os
from urllib import request
from lxml import etree
from urllib import parse

def get_film_list(films_path):
    films = []
    with codecs.open(films_path,"a+","utf-8") as f:
        for page in range(int(94469//100)+2):
            url = "https://baike.baidu.com/wikitag/api/getlemmas"
            headers = {
                       "Host":"baike.baidu.com",
                       "User-Agent" : "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0",
                       "Accept-Encoding": "gzip, deflate, br",
                       "Referer": "https://baike.baidu.com/wikitag/taglist?tagId=3122",
                       "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                       "Cookie": "BAIDUID=363A98921B60DAD6A1D33B6227CA3625:FG=1; BIDUPSID=363A98921B60DAD6A1D33B6227CA3625; PSTM=1533616008; BDUSS=0MtSkEwOEtuVjBTcGdTWlUyTVhKLVNrRzZTdW96ajRjZn5sT2FBR0N4Q0lWQWRkSUFBQUFBJCQAAAAAAAAAAAEAAACaK7onMNLky67I9LquMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIjH31yIx99cY; Hm_lvt_55b574651fcae74b0a9f1cf9c8d7c93a=1558176600,1558495614,1558677465,1558677506; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; BK_SEARCHLOG=%7B%22key%22%3A%5B%22%E8%B1%86%E7%93%A3%22%2C%22%E5%88%98%E5%BE%B7%E5%8D%8E%22%5D%7D; H_PS_PSSID=29054_1420_21086_29063_28519_29098_28722_28963_28833_28585_29072_20718; Hm_lpvt_55b574651fcae74b0a9f1cf9c8d7c93a=1558678281; pgv_pvi=4364608512; pgv_si=s9414722560",
                       "Connection": "keep-alive"}
            #params = "limit=100&timeout=3000&filterTags=%5B%5D&tagId=3122&fromLemma=false&contentLength=40&page=%s"%(page).encode("utf-8")
            params = {"limit":100,"timeout":3000,"filterTags":[],"tagId":3122,"fromLemma":False,"contentLength":40,"page":page}
            proxies = {
                "http": "http://10.244.2.5:8099",
                "https": "http://10.244.2.5:8099"
            }    
            try:
                response = requests.post(url, data=params,headers=headers,verify=False, allow_redirects=True,proxies=proxies)
                dic_json= json.loads(response.text)
                if dic_json:
                    film = dic_json["lemmaList"]
                    for i in film:
                        f.writelines(json.dumps(i,ensure_ascii=False)+"\n")
                    films+=film
            except Exception as error:
                print(error)
                continue
    
            
    return films

def extract_baidu(selector):
    info_data = {}
    if selector.xpath('//h2/text()'):
        info_data['current_semantic'] = selector.xpath('//h2/text()')[0].replace('    ', '').replace('（','').replace('）','')
    else:
        info_data['current_semantic'] = ''
    if info_data['current_semantic'] == '目录':
        info_data['current_semantic'] = ''

    info_data['tags'] = [item.replace('\n', '') for item in selector.xpath('//span[@class="taglist"]/text()')]
    if selector.xpath("//div[starts-with(@class,'basic-info')]"):
        for li_result in selector.xpath("//div[starts-with(@class,'basic-info')]")[0].xpath('./dl'):
            attributes = [attribute.xpath('string(.)').replace('\n', '') for attribute in li_result.xpath('./dt')]
            values = [value.xpath('string(.)').replace('\n', '') for value in li_result.xpath('./dd')]
            for item in zip(attributes, values):
                info_data[item[0].replace('    ', '')] = item[1].replace('    ', '')
    #if selector.xpath('//div[starts-with(@class,"para-title")]/following-sibling::node()[position() <= count( //div[starts-with(@class,"anchor-list")]/following-sibling::node()) + 1]'):
        #for content in selector.xpath('//div[starts-with(@class,"para-title")]/following-sibling::node()[position() <= count( //div[starts-with(@class,"anchor-list")]/following-sibling::node()) + 1]'):
            #content = content.xpath('./div[@class="para"]/text()')
            #print(content)
    return info_data
    
def split_str2list(s):
    pattern = r"[\/\s\.\!_,$%^*(\"\')]+|[——()?【】“”！，。？、~@#￥%……&*（）]+"
    l= re.split(pattern,s)
    return l

def get_film_details(film_list):
        final_films_list_details=[]
        with codecs.open("film_info.json","a+","utf-8") as f:
            for film in film_list:
                try:
                    data = {}
                    data["film_name"] = film.get("lemmaTitle","")
                    data["film_baike_id"] = film.get("lemmaId","") #http://baike.baidu.com/item/%E4%B8%83%E5%AE%97%E7%BD%AA/9666463
                    data["CroppedTitle"] = film.get("lemmaCroppedTitle","")
                    try:
                        data["pic"] = film["lemmaPic"].get("url","") if "lemmaPic" in film else ""
                    except:
                        data["pic"] = film.get("lemmaUrl","") if "lemmaUrl" in film else ""
                    if "lemmaUrl" in film and len(film["lemmaUrl"])>0:
                        data["film_baike_url"] = film.get("lemmaUrl")
                        selector = etree.HTML(request.urlopen(data["film_baike_url"]).read().decode('utf-8').replace('&nbsp;', ''))
                        film_info = extract_baidu(selector)
                        if film_info:
                            data["chinese_name"] = split_str2list(film_info.get("中文名",""))
                            data["foreign_name"] = split_str2list(film_info.get("外文名",""))
                            data["other_name"] = split_str2list(film_info.get("其它译名",""))
                            data["producer area"] = split_str2list(film_info.get("制片地区",""))
                            data["faxing_company"] = split_str2list(film_info.get("发行公司",""))
                            data["chupin_company"] = split_str2list(film_info.get("出品公司",""))
                            data["director"] = split_str2list(film_info.get("导演","")) 
                            data["scriptwriter"] = split_str2list(film_info.get("编剧",""))
                            data["filmmakers"] = split_str2list(film_info.get("制片人",""))
                            data["film_type"] = split_str2list(film_info.get("类型",""))
                            data["film_star"] = split_str2list(film_info.get("主演",""))
                            data["film_release_time"] = split_str2list(film_info.get("上映时间",""))
                            data["film_piaofang"] = split_str2list(film_info.get("票房",""))
                            data["film_times"] = split_str2list(film_info.get("片长",""))
                            data["film_classification"] = split_str2list(film_info.get("分级",""))
                            data["film_language"] = split_str2list(film_info.get("对白语言",""))
                    f.writelines(json.dumps(data,ensure_ascii=False)+"\n")
                except Exception as e:
                    print(e)
                    continue
                
if __name__ == '__main__':
    #film_path = "films2.json"
    #if not os.path.exists(film_path):
        #film_list = get_film_list(film_path)
        #get_film_details(film_list)
    film_list = [{'lemmaId': 9666463, 'lemmaTitle': '七宗罪', 'lemmaCroppedTitle': '七宗罪', 'lemmaDesc': '《七宗罪》是一部由大卫·芬奇执导，布拉德·皮特、摩根·弗里曼、格温妮丝·帕特洛、...', 'lemmaUrl': 'http://baike.baidu.com/item/%E4%B8%83%E5%AE%97%E7%BD%AA/9666463', 'lemmaPic': {'width': 414, 'height': 600, 'url': 'https://gss0.bdstatic.com/94o3dSag_xI4khGkpoWK1HF6hhy/baike/w%3D168/sign=790e2d89f503738dde4a08248b1bb073/562c11dfa9ec8a13a32805c9f403918fa0ecc0ed.jpg'}}]
    get_film_details(film_list)