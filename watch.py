#!/usr/bin/env python

import os
import sys
import subprocess
import datetime
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

BASEDIR = os.path.abspath(os.path.dirname(__file__))


def get_now():
    "Get the current date and time as a string"
    return datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

def build_docs():
    """
    Run the Sphinx build (`make html`) to make sure we have the
    latest version of the docs
    """

    print >> sys.stderr, "Building docs at %s" % get_now()
    os.chdir(os.path.join(BASEDIR, "docs"))
    subprocess.call(r'make.bat html')

def run_tests():
    "Run unit tests with unittest."

    print BASEDIR
    os.chdir(BASEDIR)
    print >> sys.stderr, "Running unit tests at %s" % get_now()
    subprocess.call('python -m unittest discover -b test', shell=True)

    #print >> sys.stderr, "Running nodetests at %s" % get_now()
    #subprocess.call(['nosetests', '-v'], shell=True)


def run_app():
    print BASEDIR
    os.chdir(BASEDIR)
    subprocess.call('./start_mc.py') ##, shell=True)
    


def getext(filename):
    "Get the file extension."

    return os.path.splitext(filename)[-1].lower()



class ChangeHandler(FileSystemEventHandler):
    """
    React to changes in Python and Rest files by
    running unit tests (Python) or building docs (.rst)
    """

    def on_any_event(self, event):
        "If any file or folder is changed"
        if event.is_directory:
            return
        if getext(event.src_path) == '.py':
	    if self.app and self.app == 'app':
		run_app()
	    else:
		run_tests()
        elif getext(event.src_path) == '.rst':
            build_docs()
 

def main():
    """
    Called when run as main.
    Look for changes to code and doc files.
    """

    while 1:
   
        event_handler = ChangeHandler()
        event_handler.app = None
	if len(sys.argv) > 1:
	    event_handler.app = sys.argv[1]
        observer = Observer()
        observer.schedule(event_handler, BASEDIR, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

if __name__ == '__main__':
    main()
