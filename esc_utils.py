#!/usr/bin/python

import time

class NicePrint():
    
    class LineLimitReached:
        pass
        
    def __init__(self, data = {}, keyprintstr="%s%-30s: %s", linelimit=-1):
        self.keyprintstr = keyprintstr
        self.linelimit   = linelimit
        self.linecnt     = 0
        self.shift       = ""
        
        try:
            self.doPrint(data)
        except self.LineLimitReached:
            return
        
    def doPrint(self, data):
        if type(data) == dict or type(data) == list:
            for k,v in data.iteritems():
                if self.linelimit > 0 and self.linecnt >= self.linelimit: raise self.LineLimitReached
                
                if type(v) == dict:
                    print self.keyprintstr % (self.shift, k, "")
                    self.linecnt += 1
                    self.shift = "%s  " % (self.shift) 
                    self.doPrint(v)
                    continue
                
                if type(v) == list:
                    self.shift = "%s  " % (self.shift)
                    for i,item in enumerate(v):
                        print "\n%s == Item: %i ==" % (self.shift, i)
                        self.linecnt += 1
                        self.doPrint(item)
                    print
                    continue
                
                print self.keyprintstr % (self.shift, k,v)
                self.linecnt+=1
        if type(data) == unicode:
            for line in data.splitlines():
                if self.linelimit > 0 and self.linecnt >= self.linelimit: raise self.LineLimitReached
                print line
                self.linecnt += 1
                

def arrayArgs(args):
        x = args.split(" ")
        if x[0] == "": del(x[0])
        return x

def periodic(f, sleeptime):
        try:
            cnt = 0
            while(True):
                print "\n\n%s" % (cnt)
                cnt += 1
                f()
                time.sleep(sleeptime)
                #print "\033[2J"
        except KeyboardInterrupt:
            print
            return
            
if __name__ == "__main__":
    NicePrint({"a":"4", "b":"5", "c": {"x":5, "y":3} })
    print
    NicePrint({"a":"4", "b":"5", "c": {"x":5, "y":3} }, linelimit=2)
    