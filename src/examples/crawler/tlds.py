#!/usr/bin/python

def read_tlds():
    t = set()
    for l in open('data/effective_tld_names.dat').readlines():
        l = l.strip()
        if l.startswith('//') or not l: continue
        t.add(l)
    return t
        
tlds = read_tlds()
def domain_from_site(site):
    last = -1 
    p = site.find('.')
    while p != -1:
        suffix = site[p + 1:]
        if suffix in tlds: return site[last + 1:]
        
        next = site.find('.', p + 1)
        if next != -1:             
            if '*' + site[next:] in tlds: 
                return site[last + 1:]
        
        last = p
        p = next
    return site


if __name__ == '__main__':
    print domain_from_site('www.foo.co.uk')
    print domain_from_site('a.www.foo.com')
