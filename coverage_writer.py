import sys
import os
from coverage_model.coverage import GridDomain, CRS, AxisTypeEnum, MutabilityEnum, GridShape, SimplexCoverage
from coverage_model.parameter import ParameterContext, ParameterDictionary 
from coverage_model.parameter_types import QuantityType, ArrayType 
import datetime
import numpy as np
import collections
import uuid
import time
import gevent
import getopt

class Config(object):
    pass
def config():
    return dict([(k,v) for k,v in Config.__dict__.iteritems() if k[0:2] != "__"])

class ReadWriteCoverage(object):
    
    def __init__(self):
        self.seed = "1092384956781341341234656953214543219"
        self.seed_deque = collections.deque(self.seed)
        self.size = 0
        self.storage = {}
        self.timestamp = time.time()
        self.mem_write_interval = Config.mem_write_interval
        self.mem_data_depth = Config.mem_data_depth
        self.storage_index = 0
        self.data_factor = Config.data_factor
        self.disk_path = Config.disk_path
        self.starting_free_space = self.get_disk_free_space()
        self.rebuild_percentage = Config.rebuild_percentage

    def sizeof_fmt(self, num):
        for x in ['b','KB','MB','GB']:
            if num < 1024.0 and num > -1024.0:
                return "%3.1f%s" % (num, x)
            num /= 1024.0
        return "%3.1f%s" % (num, 'TB')
    
    def get_disk_free_space(self):
        s = os.statvfs(self.disk_path)
        du = (s.f_bavail * s.f_frsize)
        return du

    def create(self, path):
        mkdir_silent(path)
        
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT, AxisTypeEnum.HEIGHT])

        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # Dimensionality is excluded for now
            
        pdict = ParameterDictionary()
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.axis = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        dens_ctxt = ParameterContext('data_quantity', param_type=QuantityType(value_encoding=np.float32))
        dens_ctxt.uom = 'unknown'
        dens_ctxt.fill_value = 0x0
        pdict.add_context(dens_ctxt)
        
        serial_ctxt = ParameterContext('data_array', param_type=ArrayType())
        serial_ctxt.uom = 'unknown'
        serial_ctxt.fill_value = 0x0
        pdict.add_context(serial_ctxt)
       
        guid = str(uuid.uuid4()).upper()

        self.path = path
        self.cov = SimplexCoverage(path, guid, name='test_cov', parameter_dictionary=pdict, temporal_domain=tdom, spatial_domain=sdom)

    def record_data(self, key, (start, end, data)):
        if time.time() - self.timestamp >= self.mem_write_interval:
            self.timestamp = time.time()
            try:
                self.storage[key].append((start, end, data))
            except KeyError:
                self.storage[key] = []
                self.storage[key].append((start, end, data))
            if self.storage_index == 0:
                self.storage[key] = []
            self.storage_index = (self.storage_index + 1) % self.mem_data_depth
    
    def format_data(self, file_data, mem_data):
        format_limit = 5
        result = []
        length_file = len(file_data)
        length_mem  = len(mem_data)
        if length_file != length_mem:
            result.append("%s != %s memory and file data length do not match" % (length_file, length_mem))
        file_string = ','.join([str(d) for i,d in enumerate(file_data) if i < format_limit])
        mem_string = ','.join([str(d) for i,d in enumerate(mem_data) if i < format_limit])
        f_limit_ind = "..." if length_file > format_limit else ""
        m_limit_ind = "..." if length_mem > format_limit else ""
        result.append("coverage [%s%s] != memory [%s%s]" % (file_string, f_limit_ind, mem_string, m_limit_ind))
        return '\n'.join(result)

    def read(self):
        size_bytes = 0
        for root, dirname, files in os.walk(self.path):
            for fname in files:
                size_bytes = size_bytes + os.path.getsize(os.path.join(root, fname))

        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        result = True
        info = []
        for key,store in self.storage.iteritems():
            for start,end,store_data in store:
                data = np.array(self.cov.get_parameter_values(key, slice(start, end)))
                result = np.allclose(data, store_data)
                if result == False:
                    info.append("%s %s"%(key,self.format_data(data, store_data)))
                    break

        du = self.get_disk_free_space()
        percentage = int((float(du) / float(self.starting_free_space)) * 100)
        if result == True:
            print "[%s] %s/%s %s%% read checkpoint passed" % (timestamp, self.sizeof_fmt(size_bytes), self.sizeof_fmt(du), percentage) 
        else:
            print "[%s] %s/%s %s%% read checkpoint FAILED" % (timestamp, self.sizeof_fmt(size_bytes), self.sizeof_fmt(du), percentage)
            print '\n'.join(info)
        
        if percentage <= self.rebuild_percentage:
            print "rebuild percentage %s%% threshold met" % self.rebuild_percentage
            self.destroy()
            self.cleanup()
            self.create(self.path)
            self.size = 0
            self.storage = {}
            self.starting_free_space = self.get_disk_free_space()

    def cleanup(self):
        print "cleaning up"
        import shutil
        shutil.rmtree(self.path)

    def _write_floats_to_cov(self, nt, seed):
        for name,(n,pc) in self.cov.parameter_dictionary.iteritems():
            if isinstance(pc.param_type, QuantityType):
                if name != "time":
                    data = np.random.uniform(0, seed, (nt,)) 
                    self.cov.set_parameter_values(name, data, slice(self.size,self.size+nt))
                    self.record_data(name, (self.size,self.size+nt,data))

    def write(self):
        seed = int(self.seed_deque[0])
        nt = (seed+1)*self.data_factor
        debug("inserting timesteps", nt)
        self.cov.insert_timesteps(nt)
        self._write_floats_to_cov(nt, seed)
        self.seed_deque.rotate(1)        
        self.size = self.size + nt

    def destroy(self):
        self.cov.close()

def mkdir_silent(newdir):
    if os.path.isdir(newdir):
        pass
    elif os.path.isfile(newdir):
        raise OSError("a file with the same name as the desired " \
            "dir, '%s', already exists." % newdir)
    else:
        head, tail = os.path.split(newdir)
        if head and not os.path.isdir(head):
            mkdir_silent(head)
        if tail:
            os.mkdir(newdir)

def debug(*args):
    if Config.debug:
        print ' '.join(args)

def produce_write():
    fast_write_mode = False
    if Config.write_interval is None:
        fast_write_mode = True
    event = gevent.event.Event()
    while True:
        try:
            if fast_write_mode == False:
                event.wait(timeout=Config.write_interval)
                q.put(rw.write)
                event.clear()
            else:
                q.put(rw.write)
        except Exception, e:
            print "produce write exception", e
            import traceback
            traceback.print_exc()
        finally:
            gevent.sleep(0)

def produce_read():
    event = gevent.event.Event()
    while True:
        try:
            event.wait(timeout=Config.read_interval)
            q.put(rw.read)
            event.clear()
        except Exception, e:
            print "produce read exception", e
            import traceback
            traceback.print_exc()
        finally:
            gevent.sleep(0)

def consume():
    while True:
        try:
            func = q.get()
            if Config.debug:
                print "running", func.__name__
            func()
        except IOError, e:
            print e
        except Exception, e:
            print "consume exception", e
            import traceback
            traceback.print_exc()
        finally:
            gevent.sleep(0)


def init_config():
    #path to coverage
    Config.coverage_path = os.path.join(os.getcwd(), 'cov_tests')
    #frequency in seconds - how often coverage data is compared against data stored in memory
    Config.read_interval = 30
    #frequency in seconds - how often data is written to coverage
    Config.write_interval = 10
    #frequency in seconds - how often data is written to memory to be compared against
    Config.mem_write_interval = 1
    #length of list - how much data to be stored in memory
    Config.mem_data_depth = 10
    #multiplicative factor for data - how much data to be written to coverage (e.g. factor of 10, factor of 100)
    Config.data_factor = 10
    #path to root of disk - available free space of path used to rebuild coverage when disk is running low
    Config.disk_path = '/'
    #percentage - what percent of disk space left to trigger a coverage rebuild
    Config.rebuild_percentage = 30
    #displays some debug
    Config.debug = False

def parse_config():
    short_names = "hp:r:w:m:"
    long_names = ["help=", "path=", "read_interval=", "write_interval=","mem_write_interval=","mem_data_depth=","data_factor=","disk_path=","rebuild_percentage=", "mode="]
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],short_names,long_names)
    except getopt.GetoptError:
        print sys.argv[0] + ' -p <path_to_coverage> -m <mode>'
        print "default coverage path:", os.path.join(os.getcwd(),'cov_tests')
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ('-h'):
            print sys.argv[0] + ' -p <path_to_coverage> -m <mode>'
            print "default coverage path:", os.path.join(os.getcwd(),'cov_tests')
            sys.exit()
        elif opt in ('--help'):
            print sys.argv[0] + ' -p <path_to_coverage> -m <mode> --read_interval <seconds> --write_interval <seconds> --mem_write_interval <seconds>' \
                                '--mem_data_depth <integer> --data_factor <integer> --disk_path <path> --rebuild_percentage <percentage>'
            print "default coverage path:", os.path.join(os.getcwd(),'cov_tests')
            sys.exit()
        elif opt in ("-m", "--mode"):
            if arg == "slow":
                Config.read_interval = 60 * 5
                Config.write_interval = 60
                Config.mem_write_interval = 60
            if arg == "medium":
                Config.read_interval = 60 * 1
                Config.write_interval = 10
                Config.mem_write_interval = 10
            if arg == "fast":
                Config.read_interval = 10
                Config.write_interval = None
                Config.mem_write_interval = 1
        elif opt in ("-p", "--path"):
            try:
                if os.path.exists(arg):
                    Config.coverage_path = arg
            except:
                pass
        elif opt in ("-r", "--read_interval"):
            try:
                Config.read_interval = int(arg)
            except:
                pass
        elif opt in ("-w", "--write_interval"):
            try:
                Config.write_interval = int(arg)
            except:
                pass
        elif opt in ("--mem_write_interval"):
            try:
                Config.mem_write_interval = int(arg)
            except:
                pass
        elif opt in ("--mem_data_depth"):
            try:
                Config.mem_data_depth = int(arg)
            except:
                pass
        elif opt in ("--data_factor"):
            try:
                Config.data_factor = int(arg)
            except:
                pass
        elif opt in ("--disk_path"):
            try:
                Config.disk_path = int(arg)
            except:
                pass
        elif opt in ("--rebuild_percentage"):
            try:
                Config.rebuild_percentage = int(arg)
            except:
                pass
        elif opt in ("--debug"):
            try:
                Config.debug = bool(arg)
            except:
                pass

if __name__ == "__main__":
    init_config()
    parse_config()
    print "using config: ", config()
    try:
        rw = ReadWriteCoverage()
        rw.create(Config.coverage_path)
        q = gevent.queue.Queue(maxsize=10)
        c = gevent.spawn(consume)
        w = gevent.spawn(produce_write)
        r = gevent.spawn(produce_read)
        gevent.joinall([c,w,r])
        #gevent.joinall([gevent.spawn(consume), gevent.spawn(produce_write), gevent.spawn(produce_read)])
    except Exception, e:
        print e
        import traceback
        traceback.print_exc()
    finally:
        rw.destroy()
        rw.cleanup()
