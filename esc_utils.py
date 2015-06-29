#!/usr/bin/python

import time

def niceprint(data = {}, keyprintstr="%s%-30s: %s",shift=""):
    for k,v in data.iteritems():
        if type(v) == type({}):
            print keyprintstr % (shift, k, "")
            niceprint(v, shift="%s  " % shift)
            continue
        
        print keyprintstr % (shift, k,v)

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
    niceprint({"a":"4", "b":"5", "c": {"x":5, "y":3} })
    
    