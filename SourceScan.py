import os
import subprocess
from time import sleep


class SourceScan:
    """ managen von dateien und verzeichnissen in denen 
        die quelldaten zu finden sind """

    def scan_source_dirs_all(self):
        """ nur subdirs erfassen """
        self.src_dirs = os.listdir(self.src_main_dir)


    def scan_sources(self):
        """ scan dir of cur set labor 
        """
        self.src_dir = self.src_main_dir + self.src_labor + '/'
        ls = os.listdir(self.src_dir)
        if 'archiv' in ls:
            ls.remove('archiv')
        ls.sort()
        self.ls = ls
        if len(ls) == 0:
            return False
        return ls


    def scan_source_dir(self, src_sub_dir):
        """ ein labor-subdir scannen, 
            @return tuple (filelist, number_files) """
        src_dir = self.src_main_dir + src_sub_dir + '/'
        ls = os.listdir(src_dir)
        if 'archiv' in ls:
            ls.remove('archiv')
        ls.sort()
        return (ls, len(ls))


    def source_load_first(self):
        """ load first 
 
        """
        ls = self.scan_sources() # XXX rename to : source_a.....
        if ls:
            self.src_cur = self.src_dir + ls[0]
        else:
            self.src_cur = None


    def source_cur_checkvalid(self):
        """ pruefe ob erstes file okay ist """
        self.source_load_first()
        fsrc = file(self.src_cur, 'r')

        suc = subprocess.call(['xmlstarlet', 'val', self.src_cur])
        if suc == 0:
            return True
        else:
            return False


    def no_files_left(self):
        """ sind alles src dirs leer """
        todo = False
        self.src_sub_dirs_todo = []
        for src_sub_dir in self.src_dirs:
            num_files, away = self.scan_source_dir(src_sub_dir)
            if num_files > 0:
                todo = True
                self.src_sub_dirs_todo.append(src_sub_dir)

        return not todo

    def loop_src_dirs(self):
        for src_sub_dir in self.src_dirs:
            self.loop_src_dir(src_sub_dir)      


    def loop_src_dir(self, src_sub_dir):
        """ verarbeite alle files in sub dir """
        ls, num = self.scan_source_dir(src_sub_dir)
        src_sub_dir_cur = self.src_main_dir + src_sub_dir
        for fn in ls:
            self.src_cur = src_sub_dir_cur + '/' + fn
            print "STARTING WORK ON FILE: "+src_sub_dir+'/'+fn
            success = self.work()
            if success:
                pass
                # XXX move file to archiv
            sleep(1)
