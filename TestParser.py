import os
import subprocess
from time import sleep
from Datensatz import Datensatz, DataStore
from lxml import etree
from lxml.etree import iterparse, XMLSyntaxError, ParseError


class MissingDataException(Exception): pass
class ImporterError(Exception):         pass
class FormatError(Exception):       pass


## helper functs
def now():
    now = datetime.now()
    #now_iso = datetime.isoformat(now)
    return now #now_iso



class TestParser:

    def __init__(self):
        self.src_cur = ''
        self.src_dir = '/path/to/dir/with/xml-files'
        self.DATASET_TAG = 'customer'

    def loop(self):
        ls = os.listdir(self.src_dir)
        ls.remove('archiv')
        for f in ls:
            self.src_cur = self.src_dir+f
            self.work()



    def parse(self):
        """ parse current source file, check encoding """
        try:
            tree = etree.parse(self.src_cur)
            self.encoding = tree.docinfo.encoding
            self.root = tree.getroot()
        except ParseError:
            raise ImporterError


    def work(self):
        self.parse()
        print self.src_cur
        #self.encoding = 'iso8859-1'
        xmlfile = open(self.src_cur)
        with xmlfile as xml_doc:
            context = iterparse(xml_doc, events=("start", "end"))

            #self.iter_context(context)
            indata = False
            if True:
            #try:
                for event, elem in context:
                    #print elem.tag
                    if elem.tag == self.DATASET_TAG:
                        if event == "start":
                            pass
                        #    data_in = Datensatz()
                        elif event == "end":
                            try:
                                data_in = Datensatz()
                                print "start FOR"
                                for child in elem:
                                    print child.tag,
                                    if child.text is None:
                                        val = None
                                    if type(child.text) == type(u""):
                                        val = child.text
                                    if type(child.text) == type(""):
                                        val = "STRING"
                                    #    val = child.text.decode(self.encoding)
                                    data_in.data[child.tag] = val
                                data_in.dump()
                                #etree.dump(elem)
                                print
                            except:
                                etree.dump(elem)
                    elif elem.tag == 'custLIS':
                        if event == "start":
                            print "START LIS"
                        elif event == "end":
                            print "custLIS ---"
                    if elem.getparent() is None:
                        break
                src_success = True
            '''
            except:
                src_success = False
                etree.dump(elem)
                sleep(3)
            '''
    
        
        print "src_success: "+str(src_success)
        print self.encoding
        return src_success


if __name__ == '__main__':
    t = TestParser()
    t.loop()
    #t.work()

