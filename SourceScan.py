import os, glob
import subprocess
from time import sleep
from lxml import etree
import logging as l



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
        l.debug('scan source sub dir: %s' %src_sub_dir )
        if self.config['src_pattern']:
            ls, ll = self.scan_source_dir_regexp(src_sub_dir)
        elif self.config['src_glob']:
            ls, ll = self.scan_source_dir_glob(src_sub_dir)
        l.info( "FOUND %s src files." % str(ll) )
        return ls, ll

    
    def scan_source_dir_glob(self, src_sub_dir):
        glob = self.config['src_glob']
        src_dir = self.src_main_dir + src_sub_dir + '/'
        ls = glob.glob( src_dir + glob )
        ls.sort()
        return (ls, len(ls))


    def scan_source_dir_regexp(self, src_sub_dir):
        l.info("REGEXP scan")
        import re
        src_dir = self.src_main_dir + src_sub_dir + '/'
        ls = os.listdir(src_dir)
        l.debug('all files in %s' %src_dir)
        l.debug('\n'.join(ls))
        pattern = re.compile(self.config['src_pattern'])
        found = []
        
        for fn in ls:
            m = re.match(pattern, fn)
            l.debug('matches %s ? - %s' %(fn, str(m)) )
            if m:
                out = fn, "\t", m.group()
                l.debug(out)
                found.append(src_dir + fn)
            else:
                print fn

        found.sort()
        return (found, len(found))
        


    def source_load_first(self):
        """ load first """
        ls = self.scan_sources() # TODO: rename to : source_a.....
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


    def source_cur_dump(self):
        etree.dump(self.root)


    def no_files_left(self):
        """ sind alles src dirs leer """
        todo = False
        self.src_sub_dirs_todo = []
        for src_sub_dir in self.src_dirs:
            num_files, away = self.scan_source_dir(src_sub_dir)
            if len(num_files) > 0:
                #print num_files
                todo = True
                self.src_sub_dirs_todo.append(src_sub_dir)
        l.debug('src_sub_dirs_todo: %s' % self.src_sub_dirs_todo)

        return not todo


    def loop_src_dirs(self):
        for src_sub_dir in self.src_dirs:
            self.loop_src_dir(src_sub_dir)      


    def loop_src_dir(self, src_sub_dir):
        """ verarbeite alle files in sub dir """
        ls, num = self.scan_source_dir(src_sub_dir)
        src_sub_dir_cur = self.src_main_dir + src_sub_dir

        for fp in ls:
            
            #self.src_cur = src_sub_dir_cur + '/' + fn
            self.src_cur = fp
            self.src_cur_archive = src_sub_dir_cur + '/archiv/' + os.path.basename(fp)
            #print "STARTING WORK ON FILE: "+src_sub_dir+'/'+fn
            l.info("STARTING WORK ON FILE: " + fp)
            success = self.work()

            if self.test:
                continue
            
            print "STARTING REAL MOVING OF FILES to archive"
            
            if success:
                # TODO: move file to archiv
                l.info( "moving file to %s " %self.src_cur_archive)
                try:
                    os.rename(self.src_cur, self.src_cur_archive)
                    l.info( "....OK!" )               
                except OSError:
                    l.error( "rename FAILED!" )
                    
            else:
                l.debug( "file failed %s " % self.src_cur)
            sleep(1)


