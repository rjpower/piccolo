#!/usr/bin/python

import json, os, re, socket, sys, time, types, threading, traceback
import gzip, urllib, cgi, httplib, urllib2, cStringIO

from urlparse import urlparse, urljoin 
from collections import defaultdict

from lxml import etree
html_parser = etree.HTMLParser()

href_xpath = etree.XPath('//a/@href')
base_xpath = etree.XPath('//base/@href')

from threading import Thread, Lock
from Queue import PriorityQueue, Queue, Empty, Full  

from tlds import domain_from_site

sys.path += ['build']
sys.path += ['build/client/python']

try:
  from piccolo import *
except:
  print 'Failed to import crawler support module!'
  traceback.print_exc()
  sys.exit(1)

if __name__ == '__main__':
	if not hasattr(NetworkThread.Get(),'size()'):
		Init(sys.argv)

num_crawlers = NetworkThread.Get().size() - 1
crawler_id = NetworkThread.Get().id()

import logging
LOGLEVEL = logging.WARN

os.system('mkdir -p logs.%d' % num_crawlers)
logging.basicConfig(level=LOGLEVEL)
                    #filename='logs.%d/crawl.log.%s.%d' % (num_crawlers, socket.gethostname(), os.getpid()),
                                   
def now(): return time.time()
def logpretreat(fmt): return str(kernel().current_shard()) + ' ' + str(fmt)
def urlescape(url_s): return url_s.replace('%','%%')

def debug(fmt, *args, **kwargs): logging.debug(logpretreat(fmt) % args, **kwargs)
def info(fmt, *args, **kwargs): logging.info(logpretreat(fmt) % args, **kwargs)
def warn(fmt, *args, **kwargs): logging.warn(logpretreat(fmt) % args, **kwargs)
def error(fmt, *args, **kwargs): logging.error(logpretreat(fmt) % args, **kwargs)
def fatal(fmt, *args, **kwargs): logging.fatal(logpretreat(fmt) % args, **kwargs)
def console(fmt, *args, **kwargs):
  if LOGLEVEL == logging.INFO or LOGLEVEL == logging.DEBUG:
    logging.info(logpretreat(fmt) % args, **kwargs)
    print >> sys.stderr, logpretreat(fmt) % args

THREADS_PER_WORKER = 1
CRAWLER_THREADS = 5
ROBOTS_REJECT_ALL = '/'
MAX_ROBOTS_SIZE = 50000
MAX_PAGE_SIZE = 100000
CRAWL_TIMEOUT = 10
RECORD_HEADER = '*' * 30 + 'BEGIN_RECORD' + '*' * 30 + '\n'
PROFILING = False
RUNTIME = -1
POLITE_INTERVAL = 30
crawl_queue = Queue(100000)
robots_queue = Queue(100000)
running = True

fetch_table = None
crawltime_table = None
robots_table = None
domain_counts = None
fetch_counts = None

class CrawlOpener(object):
  def __init__(self):
    self.o = urllib2.build_opener()
  
  def read(self, url_s):  
    debug("Fetching URL %s" % url_s)
    req = urllib2.Request(url_s)
    req.add_header('User-Agent', 'MPICrawler/0.1 +http://kermit.news.cs.nyu.edu/crawler.html')
    req.add_header('Accept-encoding', 'gzip')
    try:
      url_f = self.o.open(req, timeout=CRAWL_TIMEOUT)
      sf = cStringIO.StringIO(url_f.read())    
      if url_f.headers.get('Content-Encoding') and 'gzip' in url_f.headers.get('Content-Encoding'):
    	return gzip.GzipFile(fileobj=sf).read()
      return sf.read()
    except IOError:
      return ""
    except:
      error("Unknown open error")
      return ""
       
crawl_opener = CrawlOpener()


def enum(c):
  c.as_str = dict([(v, k) for k, v in c.__dict__.items() if not k.startswith('_')])
  return c
  
@enum
class FetchStatus:
  SHOULD_FETCH = 0
  FETCHING = 1
  FETCH_DONE = 2
  PARSE_DONE = 3
  DONE = 4
  ROBOTS_BLACKLIST = 5
  FETCH_ERROR = 6  
  PARSE_ERROR = 7
  GENERIC_ERROR = 8
  # used for reporting bytes downloaded
  FETCHED_BYTES = 10

  
@enum
class FetchCheckStatus:
  CRAWLING = 0
  PENDING = 1
  BLACKLIST = 2

@enum
class ThreadStatus:
  IDLE = 1
  FETCHING = 2
  EXTRACTING = 3
  ADDING = 4
  ROBOT_FETCH = 5 
  ROBOT_PARSE = 6

@enum
class RobotStatus:
  FETCHING = 'FETCHING'

class LockedFile(object):
  def __init__(self, f, *args, **kw): 
    self.lock = Lock()
    self.f = open(f, 'w')
  
  def writeRecord(self, data, **kw):
    with self.lock:
      self.f.write(RECORD_HEADER)
      for k, v in kw.iteritems(): self.f.write('%s: %s\n' % (k, v))
      self.f.write('length: %s\n' % len(data))
      self.f.write(data)
      self.f.write('\n')
  
  def write(self, s):
    with self.lock: self.f.write(s)

  def sync(self):
    with self.lock: os.fsync(self.f)

url_log = LockedFile('/scratch/urls.out.%d' % crawler_id)
data_log = LockedFile('/scratch/pages.out.%d' % crawler_id)

def key_from_site(site): return domain_from_site(site) + ' ' + site
  
def key_from_url(url): return domain_from_site(url.netloc) + ' ' + url.geturl()
def url_from_key(key): return urlparse(key[key.find(' ') + 1:])

import cPickle
def CrawlPickle(v): 
  return cPickle.dumps(v)

def CrawlUnpickle(v):
  return cPickle.loads(v)

class Page(object):
  @staticmethod
  def create(url):
    p = Page()
    p.domain = domain_from_site(url.netloc)
    p.url = url
    p.url_s = url.geturl()
    p.content = None
    p.outlinks = []
    return p
  
  def key(self): return key_from_url(self.url)

def update_tables():
  for t in [fetch_table, fetch_counts, crawltime_table, domain_counts, robots_table]:
    t.SendUpdates()
    t.HandlePutRequests()

def check_robots(url):
  k  = key_from_site(url.netloc)
  if not robots_table.contains(k):
    fatal('Robots error!!!! %s', url)
  
  disallowed = robots_table.get(k).split('\n')
  for d in disallowed:
    if not d: continue
    if url.path.startswith(d): return False
  return True

DISALLOW_RE = re.compile('Disallow:(.*)')
def fetch_robots(site):
  info('Fetching robots: %s', site)
  try:
    data = crawl_opener.read('http://%s/robots.txt' % site)
    info('data: %s', data)
    return data
  except urllib2.HTTPError, e:
    if e.code == 404: return ''
  except: pass
  
  info('Failed robots fetch for %s.', site, exc_info=1)
  warn('Failed robots fetch for %s: %s', site, sys.exc_info()[1])
  return ROBOTS_REJECT_ALL
    
def parse_robots(site, rtxt):
  debug('Parsing robots: %s', site)
  try:
    disallow = {}
    for l in rtxt.split('\n'):
      g = DISALLOW_RE.search(l)
      if g:
        path = g.group(1).strip()
        if path.endswith('*'): path = path[:-1]
        if path: disallow[path] = 1
        
    robots_table.update(key_from_site(site), '\n'.join(disallow.keys()))
    debug('Robots fetch of %s successful.', site)
  except:
    warn('Failed to parse robots file!', exc_info=1)

def update_fetch_table(key, status, byte_count=0): #, inside_trigger=0):
    #if crawler_triggers() and inside_trigger:
    #  fetch_table.enqueue_update(key, status)
    #  fetch_counts.enqueue_update(FetchStat.as_str[status], 1)
    #  if byte_count > 0: 
    #    fetch_counts.enqueue_update(FetchStatus.as_str[FetchStatus.FETCHED_BYTES], byte_count)
    #else:
    #console("Updating fetch table with %s -> %d" % (key,status))
    fetch_table.update(key, status)
    fetch_counts.update(FetchStatus.as_str[status], 1)
    if byte_count > 0:
      fetch_counts.update(FetchStatus.as_str[FetchStatus.FETCHED_BYTES], byte_count)

def fetch_page(page):
  info('Fetching page: %s', page.url_s)
  try:
    page.content = crawl_opener.read(page.url_s)
    url_log.write(page.url_s + '\n')
    data_log.writeRecord(page.content, url=page.url_s, domain=page.domain)
    update_fetch_table(page.key(), FetchStatus.FETCH_DONE, len(page.content))
    info('Fetched page: %s', page.url_s)
  except (urllib2.URLError, socket.timeout):
    info('Fetch of %s failed: %s', page.url_s, sys.exc_info()[1])
    warn('Fetch of %s failed.', page.url_s, exc_info=1)
    page.content = ''
    update_fetch_table(page.key(), FetchStatus.FETCH_ERROR)

def join_url(base, l):
  '''Join a link with the base href extracted from the page; throwing away garbage.'''   
  if l.find('#') != -1: l = l[:l.find('#')]
  j = urljoin(base, l)
  if len(j) > 200 or \
     len(j) < 6 or \
     ' ' in j or not \
     j.startswith('http://') or \
     j.count('&') > 2: return None
  return j

def extract_links(page):
  debug('Extracting... %s [%d]', page.url_s, len(page.content))
  if not page.content:
  	page.outlinks = []
  	return
  
  try:
    root = etree.parse(cStringIO.StringIO(page.content), html_parser)
    base_href = base_xpath(root)
    if base_href: base_href = base_href[0].strip()
    else: base_href = page.url_s.strip()
    
    outlinks = [join_url(base_href, l) for l in href_xpath(root)]
    page.outlinks = set([l for l in outlinks if l])
  except:
    info('Parse of %s failed: %s', page.url_s, sys.exc_info()[1])
    warn('Parse of %s failed.', page.url_s, exc_info=1)
    update_fetch_table(page.key(), FetchStatus.PARSE_ERROR)
    

def add_links(page):
  for l in page.outlinks:
    debug('Adding link to fetch queue: %s', l)
    if not isinstance(l, types.StringType): info('Garbage link: %s', l)
    else: fetch_table.update(key_from_url(urlparse(l)), FetchStatus.SHOULD_FETCH)

class CrawlThread(Thread):  
  def __init__(self, id): 
    Thread.__init__(self)
    self.status = ThreadStatus.IDLE
    self.url = ''
    self.last_active = now()
    self.id = id
    
  def ping(self, new_status):
    self.last_active = now()
    self.status = new_status
  
  def run(self):
    while 1:
      self.url = ''
      self.ping(ThreadStatus.IDLE)
      
      if robots_queue.empty() and crawl_queue.empty():
        time.sleep(1)
      
      for i in range(10):
        page = None
        try:
          page = crawl_queue.get(block=False)
        except Empty: continue
        
        if page:
          try:
            self.url = page.url_s          
            self.ping(ThreadStatus.FETCHING) 
            fetch_page(page)
            self.ping(ThreadStatus.EXTRACTING)
            extract_links(page)
            self.ping(ThreadStatus.ADDING)
            add_links(page)
            fetch_table.update(page.key(), FetchStatus.DONE)
          except:
            warn('Error when processing page %s', page.url_s, exc_info=1)
            update_fetch_table(page.key(), FetchStatus.GENERIC_ERROR)
          finally:
            crawl_queue.task_done()
            
      site = None      
      try:
        site = robots_queue.get(block=False)
      except Empty: continue
      
      if site:
        try:        
          self.url = site
          self.ping(ThreadStatus.ROBOT_FETCH)
          robots_data = fetch_robots(site)
          self.ping(ThreadStatus.ROBOT_PARSE)
          parse_robots(site, robots_data)
        except:
          warn('Error while processing robots fetch!', exc_info=1)
        finally:
          robots_queue.task_done()
      

class StatusThread(Thread):
  def __init__(self, threadlist):
    Thread.__init__(self)
    self.threads = threadlist
    self.start_time = time.time()
    self.end_time = time.time() + RUNTIME
  
  def print_status(self):
      global running
      console('Crawler status [running for: %.1f, remaining: %.1f, running: %d]',
          time.time() - self.start_time, self.end_time - time.time(), running)
      
      console('Page queue: %d; robot queue %d',
          crawl_queue.qsize(), robots_queue.qsize())
      
      it = fetch_counts.get_iterator(kernel().current_shard())
      while not it.done():            
        console('Fetch[%s] :: %d', it.key(), it.value())
        it.Next()
      
      console('Threads (ordered by last ping time)')
      for t in sorted(self.threads, key=lambda t: t.last_active):
        console('>> T(%02d) last ping: %.2f status: %s last url:%s',
                t.id, now() - t.last_active, ThreadStatus.as_str[t.status], t.url)
        
      it = domain_counts.get_iterator(kernel().current_shard())
      dcounts = []
      while not it.done(): 
        dcounts += [(it.key(), it.value())]
        it.Next()
      
      console('Top sites:')
      dcounts.sort(key=lambda t: t[1], reverse=1)
      for k, v in dcounts[:10]:
        console('%s: %s', k, v)
    
  def run(self):
    global running
    while 1:
      try:
        if crawler_id == 1:
          self.print_status()
      except:
        warn('Failed to print status!?', exc_info=1)
            
      update_tables()
      if RUNTIME > 0 and time.time() - self.start_time > RUNTIME:
        running = False
      
      url_log.sync()
      data_log.sync()
      time.sleep(5)


def crawl():  
  global RUNTIME
  global running
  RUNTIME = crawler_runtime()
  
  threads = [CrawlThread(i) for i in range(CRAWLER_THREADS)]
  for t in threads: t.start()
  
  status = StatusThread(threads)
  status.start()
  
  warn('Starting crawl!')
  last_t = time.time()
  
  while running:
    it = fetch_table.get_iterator(kernel().current_shard())
    if time.time() - last_t > 10:
      warn('Queue thread running...')
      last_t = time.time()
    
    while not it.done() and running:
      url = url_from_key(it.key())
      status = it.value()
      
      debug('Looking AT: %s %s', url, status)      
      if status == FetchStatus.SHOULD_FETCH:
        check_url(url, status)
      it.Next()
    
    time.sleep(0.1)
    
def fetchadd(current,update):
  console("Fetch Accumulator: %d %d" % (current, update))
  
  if update > current: #max!
    current = update
  return (current, (update < FetchStatus.DONE))

def fetchtrigger(k,lastrun):
  url = url_from_key(k)
  value = fetch_table.get(k)
  console("Fetch Trigger: %s %d -> %d" % (k, lastrun,value))
  if value == FetchStatus.SHOULD_FETCH:
    console("Calling check_url with (%s,%d)" % (url.geturl(),value))
    fetchchecked_status = check_url(url, value)
    if fetchchecked_status == FetchCheckStatus.PENDING:
      console("%s not yet ready, setting long trigger" % (url.geturl()))
      return True
    elif fetchchecked_status == FetchCheckStatus.CRAWLING:
      console("%s is now being CRAWLED" % (url.geturl()))
    else:
      console("%s has been blacklisted" % (url.geturl()))
  elif value == FetchStatus.FETCHING:
    console("%s determined to be crawlable" % (url.geturl()))
    page = Page.create(url)
    try:
      fetch_page(page)
      extract_links(page)
      add_links(page)
      fetch_table.update(page.key(), FetchStatus.DONE)
    except:
      warn('Error when processing page %s', page.url_s, exc_info=1)
      update_fetch_table(page.key(), FetchStatus.GENERIC_ERROR)
  elif value == FetchStatus.DONE:
    warn("%s crawling complete." % urlescape(url.geturl()));
  return False

def robotsadd(current,update):
  console("Robots Accumulator: %s %s" % (current, update))
  if current == 'FETCHING':
    current = update
  return (current, (current == 'FETCHING'))

def robotscrawl(k,islast):
  console("Robots Trigger: %s %d" % (k, islast))
  value = robots_table.get(k)
  if value <> 'FETCHING':
    return False   #Already got the contents!
  url = url_from_key(k)
  site = url.geturl()
  if site:
    try:        
      console("Robots: Fetching")
      robots_data = fetch_robots(site)
      console("Robots: Parsing")
      parse_robots(site, robots_data)
    except:
      warn('Error while processing robots fetch!', exc_info=1)
    finally:
      pass
  return False

def trigger_crawl():
  global RUNTIME,LOGLEVEL
  global running
  logging.level = LOGLEVEL
  RUNTIME = crawler_runtime()

  warn('[%d] Starting trigger crawl!',kernel().current_shard())
  last_t = time.time()

  if kernel().current_shard() == 0:
    console('Adding seed page...')
    key = key_from_url(urlparse("http://www.cemetech.net/files/crawlstart.html"))
    print(logging.Logger.root)
    update_fetch_table(
      key,
      FetchStatus.SHOULD_FETCH)
    print(logging.Logger.root)
    logging.Logger.root.parent = None
    #console("[0] Waiting for page...")
    #while not(fetch_table.contains(key)):
    #  update_tables()
    #  time.sleep(0.05)
    console("[0] Page added.")

def blocking_crawl():
  global RUNTIME
  global running
  RUNTIME = crawler_runtime()
  
  threads = [CrawlThread(i) for i in range(CRAWLER_THREADS)]
  for t in threads: t.start()
  
  status = StatusThread(threads)
  status.start()
  
  warn('Starting crawl!')
  last_t = time.time()
  
  
  it = fetch_table.get_iterator(kernel().current_shard())
  local = set()
  while not it.done():
    local.add(it.key())
    it.Next()
    
  done = False  
  while not done and running:
    done = True
    
    for url in local:   
      status = fetch_table.get(url)
      info('Looking AT: %s %s', url, status)
      
      url = url_from_key(url)
      
      if status == FetchStatus.SHOULD_FETCH:
        check_url(url, status)
      if status < FetchStatus.DONE:
        done = False
    
    time.sleep(0.1)
  
  robots_queue.join()
  crawl_queue.join() 


def check_url(url, status):
  url_s = url.geturl()
  site = url.netloc
  robots_key = key_from_site(site)

  console('Checking: %s %s', url_s, status)
    
  if not robots_table.contains(robots_key):
    console('Queueing robots fetch: %s', site)    
    #if crawler_triggers():
    #  robots_table.enqueue_update(robots_key, RobotStatus.FETCHING)
    #else:
    robots_table.update(robots_key,RobotStatus.FETCHING)
    if not crawler_triggers():
      robots_queue.put(site)
    return FetchCheckStatus.PENDING
  
  if robots_table.get(robots_key) == RobotStatus.FETCHING:
    console('Waiting for robot fetch: %s', url_s)
    return FetchCheckStatus.PENDING
  
  if not check_robots(url):
    console('Blocked by robots "%s"', url_s)
    update_fetch_table(key_from_url(url), FetchStatus.ROBOTS_BLACKLIST, 1)
    return FetchCheckStatus.BLACKLIST
    
  last_crawl = 0
  domain = domain_from_site(site)
  if crawltime_table.contains(domain):
    last_crawl = crawltime_table.get(domain)
  
  if now() - last_crawl < POLITE_INTERVAL:
    console('Waiting for politeness: %s, %d' % (url_s, now() - last_crawl))
    return FetchCheckStatus.PENDING
  else:
    console('Queueing: %s', url_s)
    if not crawler_triggers():
      crawl_queue.put(Page.create(url))
    fetch_table.update(key_from_url(url), FetchStatus.FETCHING)
    domain_counts.update(domain, 1)
    crawltime_table.update(domain, int(now()))
    crawltime_table.SendUpdates()
    crawltime_table.HandlePutRequests()
    return FetchCheckStatus.CRAWLING
    
def initialize():
  console('Initializing...')
  global fetch_table, crawltime_table, robots_table, domain_counts, fetch_counts
  fetch_table = kernel().GetIntTable(0)
  crawltime_table = kernel().GetIntTable(1)
  robots_table = kernel().GetStringTable(2) 
  domain_counts = kernel().GetIntTable(3)
  fetch_counts = kernel().GetIntTable(4)
  
  if crawler_triggers() != 1:
    if kernel().current_shard() == 1:
      console('Adding seed page...')
      fetch_table.update(
        key_from_url(urlparse("http://www.cemetech.net/files/crawlstart.html")),
        FetchStatus.SHOULD_FETCH)
  
  return 0

def main():
  global fetch_table, crawltime_table, robots_table, domain_counts, fetch_counts, robotfetchtrigid;
  num_workers = NetworkThread.Get().size() - 1

  if crawler_triggers() == 1:
    fetch_table = CreateIntTable(0,  num_workers, DomainSharding(), IntTrigger.PythonCode("fetchadd","fetchtrigger"),THREADS_PER_WORKER)
  else:
    fetch_table = CreateIntTable(0,  num_workers, DomainSharding(), IntAccum.Max())

  crawltime_table = CreateIntTable(1,  num_workers, DomainSharding(), IntAccum.Max())
  if crawler_triggers() == 1:
    robots_table = CreateStringTable(2,  num_workers, DomainSharding(), StringTrigger.PythonCode("robotsadd","robotscrawl"),THREADS_PER_WORKER)
  else:
    robots_table = CreateStringTable(2,  num_workers, DomainSharding(), StringAccum.Replace())

  domain_counts = CreateIntTable(3,  num_workers, DomainSharding(), IntAccum.Sum())
  fetch_counts = CreateIntTable(4,  num_workers, DomainSharding(), IntAccum.Sum())

  # N.B.: Trigger mode does NOT use the crawl table.
  crawl_table = CreateIntTable(5,  num_workers, DomainSharding(), IntAccum.Max())

#  robotfetchtrigid = fetch_table.py_register_trigger('RobotFetchTrigger')
#  robotretrigid = fetch_table.py_register_trigger('RobotRetrigger')
#  print "robotfetchtrigid = %d" % (robotfetchtrigid)
#  print "robotretrigid = %d" % (robotretrigid)

  conf = ConfigData()
  conf.set_num_workers(num_workers)
  if not StartWorker(conf):
    m = Master(conf)

#    if crawler_triggers() != 1:
#      m.enable_trigger(robotfetchtrigid,0,False)

#    m.enable_trigger(robotretrigid,0,False)

    if crawler_triggers() == 1:
      print fetch_table
      m.py_run_all_inttable('initialize()', fetch_table)
      m.py_run_all_inttable('trigger_crawl()', fetch_table)
    else:    
      m.py_run_all_inttable('initialize()', fetch_table)
      #m.py_run_all('crawl()', fetch_table)
      for i in range(100):
        m.py_run_all_inttable('blocking_crawl()', fetch_table)
  
if __name__ == '__main__':
  main()
