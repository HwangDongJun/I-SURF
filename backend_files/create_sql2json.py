import re


class sql2json(object):
    def __init__(self, source_file):
        self.souce_file = source_file

    def change_sql2json(self):
        with open(self.souce_file, encoding="utf-8") as fr:
            fw = open("./output_file/link.json", "w", encoding="utf-8")
            final_txt = ""
            fw.write("[\n")
            while True:
                statements = fr.readline()
                if not statements:
                    break

                if "INSERT INTO `pagelinks` VALUES" in statements:
                    slice_statements = statements[31:-2]

                    list_state = str(slice_statements).split("),(")
                    for i in range(len(list_state)):
                        comp = list_state[i].split(",")
                        # f.write(template % (i, comp[0], comp[2].split("'")[1]))
                        if "\\" in comp[2]:
                            if "\\" == comp[2]:
                                final_txt += '{"from":"%s","link_keyword":"%s"},\n' % (comp[0].replace("(",""), comp[2][1:-1].replace("\\", "[\\]").replace("\\", ""))
                            elif "\\\"" in comp[2]:
                                final_txt += '{"from":"%s","link_keyword":"%s"},\n' % (comp[0].replace("(",""), comp[2][1:-1].replace("\\\"", "").replace("\\", ""))
                        else:
                            final_txt += '{"from":"%s","link_keyword":"%s"},\n' % (comp[0].replace("(",""), comp[2][1:-1].replace("\\\'", "'"))
            fw.write(final_txt[:-2] + "\n")
            fw.write("]")
