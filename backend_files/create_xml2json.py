import os
import sys


class xml2json(object):
    def __init__(self, source_file, output_file):
        self.source_file = source_file
        self.output_file = output_file

    def extract_with_delims(self, s, start_delim, end_delim):
        start_index = s.find(start_delim)
        if start_index == -1:
            return None

        end_index = s.find(end_delim)
        if end_index == -1:
            return None

        if end_index <= start_index:
            return None

        start_index += len(start_delim)

        return s[start_index:end_index]

    def extract_data(self, part):
        title = self.extract_with_delims(part, "<title>", "</title>")
        page_id = self.extract_with_delims(part, "<id>", "</id>")
        text = self.extract_with_delims(part, "<text xml:space=\"preserve\">", "</text>")

        return (title, page_id, text)

    def json_encode_string(self, s):
        s = s.replace("\\", "\\\\")
        s = s.replace("/", "\\/")
        s = s.replace('"', '\\"')
        s = s.replace("\n", "\\n")
        s = s.replace("\r", "\\r")
        s = s.replace("\t", "\\t")
        return '"' + s + '"'

    def split_records(self):
        enwikisource = open(self.source_file, 'rt', encoding='UTF8')
        text_buffer = ""

        start_index = 0
        end_index = 0

        while True:
            chunk = enwikisource.read(16 * 1024 * 1024)

            if chunk:
                text_buffer += chunk

            start_index = 0
            end_index = 0

            while True:
                start_index = text_buffer.find("<page>", start_index)

                # No pages in the buffer, continue loading data
                if start_index == -1:
                    break

                end_index = text_buffer.find("</page>", end_index)

                # No complete page in buffer
                if end_index == -1:
                    break

                yield text_buffer[start_index:end_index + len("</page>")]

                start_index = end_index + len("</page>")
                end_index = start_index

            # No more data
            if chunk == "":
                break

            if start_index == -1:
                text_buffer = ""
            else:
                text_buffer = text_buffer[start_index:]

    def change_xml2json(self):
        if os.path.exists(self.output_file):
            print("Output file already exists. Please remove first so I don't destroy your stuff please")
            sys.exit(1)

        json_file = open(self.output_file, "w", encoding='UTF8')
        template = '{"id":%s, "title": %s, "text": %s}\n'
        # i = 0

        # json_file.write("[\n")

        try:
            for page in self.split_records():
                # i += 1
                # sys.stdout.write("\r%d" % (i,))
                # sys.stdout.flush()

                title, page_id, text = self.extract_data(page)

                if None in (title, page_id, text):
                    continue

                json_file.write(template % tuple(map(self.json_encode_string,
                                                     (page_id, title, text))))
        finally:
            # json_file.write("]\n")
            json_file.close()
