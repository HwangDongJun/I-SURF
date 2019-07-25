import os
import sys
import threading
import json
import ijson
import operator
import ast

from create_xml2json import xml2json
from create_sql2json import sql2json


# id_list = {}
# def create_search_table():
# 	with open("./output_file/simplewiki-article.json", encoding="utf-8") as f:
# 		while True:
# 			data = f.readline()
# 			if not data:
# 				break
# 			key = json.loads(data)
# 			data = f.readline()
# 			value = json.loads(data)
# 			value["title"] = value["title"].replace(" ", "_")
	#transform_title2id(id_list)

def transform_title2id(id_list):
	array = []
	with open("./output_file/link.json", encoding="utf-8") as f:
		while True:
			data = f.readline()
			if not data:
				break
			# key = json.loads(data)
			# key = str(key).replace("'", "\"")
			# data = f.readline()
			value = json.loads(data)
			if(value["link_keyword"] in id_list) :
				value["link_keyword"] = id_list[value["link_keyword"]]
				value = str(value).replace("'", "\"")

				# fw.write(key + "\n")
				# fw.write(str(value) + ",\n")
				array.append(value)

	with open('./output_file/new_link.json', 'w', encoding="utf-8") as fw:
		fw.write("[\n")
		for i in range(len(array)):
			fw.write(array[i])
			if(i != len(array) - 1):
				fw.write(",\n")
		fw.write("\n]")

def get_string_links():
	link_str = "{"
	with open("./output_file/link.json", 'r', encoding="UTF-8") as fd:
		parser = ijson.parse(fd)
		from_id = 0
		for prefix, event, value in parser:
			# print(prefix, event, value)
			if prefix.endswith('.from'):
				from_id = value
				link_str += '"%s":' % (value)
			elif prefix.endswith('.link_keyword'):
				link_str += '"%d/%s", ' % (int(from_id), value)
	print("Completed get_string_links!")
	return link_str[:-2] + '}'
	# unsorted = pd.read_json("./output_file/link.json")
	# (unsorted.sort_values("from")).to_json("./output_file/sorted_list.json")
	# sorted = unsorted.sort_values("from")
	#
	# sorted = sorted['link_keyword'].groupby([sorted["from"]]).apply(list).reset_index()
	# #print(sorted[['from', 'link_keyword']])
	# print("DataFrame make complete!")
	# sorted.to_json("./output_file/sorted_list.json", orient='records',  lines=True)

def join_table(links_data):
	id_title_dict = dict()
	for link in links_data:
		title_list = ""
		if link[0] in id_title_dict:
			id_title_dict[link[0]] = id_title_dict[link[0]] + "," + link[1]
		else:
			id_title_dict[link[0]] = link[1]
	print("Completed id_title_dict!!")
	# line_count = len(id_title_dict)
	template = '{"index":{"_type":"page","_id":"%s"}}\n{"title": "%s", "text": "%s", "link": "%s"}\n'

	ff = open("./output_file/simplewiki-article.json", "r", encoding="utf-8")

	count = 0
	file_count = 0
	while True:
		article_data = ff.readline()
		if not article_data:
			break

		article_data = ast.literal_eval(article_data)
		match_title_list = ""
		article_id = article_data["id"]
		if article_id in id_title_dict:
			match_title_list = id_title_dict[article_id]

		if count % 10000 == 0:
			file_count += 1
		open("./output_file/dataset" + str(file_count) + ".json", "a", encoding="utf-8").write(template % (article_id, article_data["title"], article_data["text"].replace("\n", " "), match_title_list))


		# if count <= int(line_count/2):
		# 	open("./output_file/dataset1.json", "a", encoding="utf-8").write(template % (article_id, article_data["title"], article_data["text"].replace("\n", " "), match_title_list))
		# else:
		# 	open("./output_file/dataset2.json", "a", encoding="utf-8").write(template % (article_id, article_data["title"], article_data["text"].replace("\n", " "), match_title_list))
		count += 1
# def join_table():
# 		dataset_list = [open("./output_file/dataset1.json", "w", encoding="utf-8"), open("./output_file/dataset2.json", "w", encoding="utf-8")]
# 		#dataset = open("./output_file/dataset.json", "w", encoding="utf-8")
# 		template = '{"index":{"_type":"page","_id":"%s"}}\n{"title": "%s", "text": "%s", "link": "%s"}\n'
#
# 		ff = open("./output_file/simplewiki-article.json", "r", encoding="utf-8").readlines()
# 		i = 0
# 		with open("./output_file/sorted_list.json", encoding="utf-8") as f:
# 			for dataset in dataset_list:
# 				if i > len(ff):
# 					continue
# 				while True:
# 					link = f.readline()
# 					if not link:
# 						break
#
# 					link = json.loads(link)
# 					while True:
# 						if(i >= len(ff)):
# 							break
# 						post = ff[i]
#
# 						post = json.loads(post)
# 						id = post["index"]["_id"]
#
# 						if(int(link["from"]) < int(id)):
# 							break
#
# 						if int(link["from"]) == int(id):
# 							post = json.loads(ff[i + 1])
# 							dataset.write(template % (id, id, post["title"], post["text"].replace("\n", " "), link["link_keyword"]))
# 							i += 2
# 							break
# 						else:
# 							post = json.loads(ff[i + 1])
# 							dataset.write(template % (id, id, post["title"], post["text"].replace("\n", " "), "[]"))
# 							i += 2
#
# 			while True:
# 				if(i >= len(ff)):
# 					break
# 				post = ff[i]
# 				post = json.loads(post)
# 				id = post["index"]["_id"]
# 				post = json.loads(ff[i + 1])
# 				dataset.write(template % (id, post["title"], post["text"], "[]"))
# 				i += 2

def transform_xml2json(input_xml_file, output_json_file):
    Txml2json = xml2json(input_xml_file, output_json_file)
    Txml2json.change_xml2json()

def transform_sql2json(input_sql_file):
    Tsql2json = sql2json(input_sql_file)
    Tsql2json.change_sql2json()

def change_files():
	SOURCE_FILE = "./xml_file/simplewiki-20190701-pages-articles.xml"
	LINK_SQL_FILE = "./sql_file/simplewiki-20190701-pagelinks.sql"
	OUTPUT_FILE = "./output_file/simplewiki-article.json"

	th1 = threading.Thread(target=transform_xml2json, args=(SOURCE_FILE, OUTPUT_FILE,))
	th2 = threading.Thread(target=transform_sql2json, args=(LINK_SQL_FILE,))
	th1.start(); th2.start()
	th1.join(); th2.join()

	# create_search_table()

	final_link_str = get_string_links()
	change_json = json.loads(final_link_str)
	sorted_links = sorted(change_json.items(), key=operator.itemgetter(0))
	join_table(sorted_links)

	# fiw = open("./output_file/join_table.json", "w", encoding="utf-8")

	# final_link_str.sort(key=extract_time, reverse=True)
	# faw = open("./output_file/sort_link.json", "w", encoding="utf-8")
	# faw.write(final_link_str)
	#join_table()

if __name__ == '__main__':
    sys.exit(change_files())
