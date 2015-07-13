#!/usr/bin/python

import cmd
import elasticsearch
import sys
import argparse
import re
import logging
import time
import readline
import os

import esc_utils


#TODO get python logger for elasticsearch
#Ideas:
# - SSH tunnel for remote connections

class ESCPrompt(cmd.Cmd):
    prompt = "ESC> "
    def __init__(self, hosts="localhost:9200", completekey='tab', stdin=None, stdout=None):
        cmd.Cmd.__init__(self, completekey=completekey, stdin=stdin, stdout=stdout)
        self.do_connect(hosts)

    def do_set_loglevel(self, args):
        """ Setup the log level, possible values: CRITICAL, ERROR, WARNING, INFO, DEBUG """
        if type(args) != str: return
        try:
            logging.getLogger().setLevel(args.upper())
        except ValueError:
            print "Possible levels: CRITICAL, ERROR, WARNING, INFO, DEBUG - case ignored"

    def do_thread_info(self, args):
        try:
            parser = argparse.ArgumentParser(prog="thread_info")
            parser.add_argument("-f", nargs="+", help="fields to show", default=[])
            parser.add_argument("--listfields",action="store_true", help="list available fields" )
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if pargs.listfields:
            esc_utils.NicePrint(self.es.cat.thread_pool(params={"help":True, "v":True}))
            return
            
        if pargs.f:
            esc_utils.NicePrint(self.es.cat.thread_pool(params={"v": True, "h": ",".join(pargs.f)}))
            return
        
        esc_utils.NicePrint(self.es.cat.thread_pool(params={"v": True}))
        

    def do_info(self, args=""):
        """ Get basic cluster info """
        esc_utils.NicePrint(self.es.info())

#    def do_segments_list(self, args):
        #""" List Lucene segments per index. """
#        esc_utils.NicePrint(self.es.cat.segments())
#I don't have it in my version, api doc says it should be here ... ???

        
    def do_cluster_get_settings(self, args):
        """ Returns cluster settings. """
        esc_utils.NicePrint(self.es.cluster.get_settings())

    def cluster_health(self):
        esc_utils.NicePrint(self.es.cluster.health())
        
    def do_cluster_health(self, args =""):
        """ Provide system health """
        try:
            parser = argparse.ArgumentParser(prog="cluster_health")
            parser.add_argument("-c", action="store_true", help="Do periodic query, use -t to set sleep time")
            parser.add_argument("-t", type=int, help="seconds between two query, use with -c", default=1)
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if pargs.c:
            esc_utils.periodic(self.cluster_health, pargs.t)
            return
        
        self.cluster_health()

    def cluster_pending_tasks(self, linelimit = -1):
        esc_utils.NicePrint(self.es.cluster.pending_tasks(), linelimit = linelimit)

    def do_cluster_pending_tasks(self, args=""):
        """ Ask the task list of the cluster. """
        try:
            parser = argparse.ArgumentParser(prog="cluster_pending_tasks")
            parser.add_argument("-c", action="store_true", help="Do periodic query, use -t to set sleep time")
            parser.add_argument("-t", type=int, help="seconds between two query, use with -c", default=1)
            parser.add_argument("-l", type=int, help="Number of lines to print", default=-1)
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if pargs.c:
            esc_utils.periodic(lambda: self.cluster_pending_tasks(pargs.l), pargs.t)
            return
        
        self.cluster_pending_tasks(pargs.l)

    def do_shards_list(self, args):
        try:
            parser = argparse.ArgumentParser(prog="shards_list")
            parser.add_argument("--state", default="all", help="Filter for given status like: started, unassigned ..etc")
            parser.add_argument("--index", default=[], help="List of index names to filter for", nargs="+")
            parser.add_argument("--grep", default=[], help="grep output with python regexp")
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        params = {"v":True}
        if pargs.index:
            params["index"] = ",".join(pargs.index)
        
        if pargs.state == "all" and not pargs.grep:
            print self.es.cat.shards(params=params)
        else:
            header=True
            for line in self.es.cat.shards(params=params).splitlines():
                if header: 
                    print line
                    header = False
                    continue
                if pargs.state != "all" and re.match(pargs.state.upper(), filter(None, line.split(" "))[3]): #Magic Do not touch
                    print line
                if pargs.grep and re.match(pargs.grep, line):
                    print line
    
    def do_cluster_put_settings(self, args):
        """ Change cluster settings, default to transistent, use -p for persistent changes """
        try:
            parser = argparse.ArgumentParser(prog="cluster_put_settings")
            parser.add_argument("-p", action="store_true", help="persistent")
            parser.add_argument("key", help="setting to change")
            parser.add_argument("value", help="value")
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        typ = "transient"
        if pargs.p:
            typ = "persistent"
        req = '{ "%s": { "%s" : %s }}' % (typ, pargs.key, pargs.value)
        
        esc_utils.NicePrint(self.es.cluster.put_settings(req))
        
    def do_nodes_info(self, args):
        esc_utils.NicePrint(self.es.nodes.info())
            
    def precmd(self, line):
        self.cmd_started=time.time()
        return line
    
    def postcmd(self, stop, line):
        self.cmd_stoped=time.time()
        print "(took: %.3f sec)" % (self.cmd_stoped - self.cmd_started)
        return line
    
    def do_index_get_settings(self,args="_all"):
        """ Get's settings of all or some indices """
        esc_utils.NicePrint(self.es.indices.get_settings(args))
            
    def do_cluster_set_disable_allocation(self, args):
        """ Set disable_allocation to true / false, feel the negative logic here ! """
        if args.lower() in [ "true", "false" ]:
            esc_utils.NicePrint(self.es.cluster.put_settings('{"transient":{"cluster.routing.allocation.disable_allocation": %s}}' % (args.lower())))
            return
        
        print "Possible vaules: true / false"
        
    def do_levitate_allocation(self, args):
        """ Okay, it's a stupid hack, if your cluster is pilling up the pending tasks after restart, you may use this than to limit the pending tasks """
        try:
            parser = argparse.ArgumentParser(prog="levitate_allocation")
            parser.add_argument("-e", required=True, type=int, help="seconds to keep the allocation enabled")
            parser.add_argument("-d", required=True, type=int, help="seconds to keep the allocation disabled, usually ~10 sec is enough to flush the pending tasks")
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        try:
            while True:
                self.do_cluster_set_disable_allocation("true")
                time.sleep(pargs.d)
                self.do_cluster_set_disable_allocation("false")
                time.sleep(pargs.e)
        except KeyboardInterrupt:
            self.do_cluster_set_disable_allocation("false")
            

    def shards_show_recovery(self, linelimit):
        esc_utils.NicePrint(self.es.cat.recovery(), linelimit = linelimit)
        
    def do_shards_show_recovery(self, args):
        try:
            parser = argparse.ArgumentParser(prog="shards_show_recovery")
            parser.add_argument("-c", action="store_true", help="Do periodic query, use -t to set sleep time")
            parser.add_argument("-t", type=int, help="seconds between two query, use with -c", default=1)
            parser.add_argument("-l", type=int, help="Number of lines to print", default=-1)
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if pargs.c:
            esc_utils.periodic(lambda: self.shards_show_recovery(pargs.l), pargs.t)
            return
        
        self.shards_show_recovery(pargs.l)

    def do_nodes_list(self, args):
        print "%s" % (self.es.cat.nodes())

    def do_search_index(self, args):
        """ Search the elasticsearch for a given index name """
        #TODO Change this to use the argparser too
        if len(args) == 0:
            print "I need at least one parameter"
            return
        try:
            res = self.es.search(index=args, body={"query": {"match_all": {}}})
            print ("Got %d hits:" % res['hits']['total'])
            for hit in res['hits']['hits']:
                print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
        except elasticsearch.exceptions.NotFoundError:
            print "No hits, sorry."

    def do_quit(self, args):
        """ESCAPE FROM ESC :D"""
        print "Quitting."
        readline.write_history_file("%s/.esc_history" % os.environ['HOME'])
        raise SystemExit

    def do_q(self, args):
        """ Shorty for for quit. """
        self.do_quit(args)
        

    def do_history(self, args):
        try:
            parser = argparse.ArgumentParser(prog="history")
            parser.add_argument("-c", action="store_true", help="clear history")
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if (pargs.c):
            readline.clear_history()
            return
        
        for i in xrange(1, readline.get_current_history_length()):
            print "%i: %s" % (i, readline.get_history_item(i))
            
    def do_redo(self, args):
        try:
            self.onecmd(readline.get_history_item(int(args)))
            return
        except ValueError:
            print "Give the index of the history, so I can re-do that command for you"

    def do_connect(self, args):
        try:
            parser = argparse.ArgumentParser(prog="connect")
        #    parser.add_argument("-c", action="store_true", help="Do periodic query, use -t to set sleep time")
            parser.add_argument("target", nargs="+")
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        self.hosts = pargs.target
        self.es = elasticsearch.Elasticsearch(self.hosts)
        ESCPrompt.prompt = "ESC %s > " % (self.hosts)

    def do_nodes_allocation(self, args):
        try:
            parser = argparse.ArgumentParser(prog="nodes_allocation")
            parser.add_argument("-e", action="store_true", help="Exclude nodes from allocation")
            parser.add_argument("-d", action="store_true", help="Empty exclude list")
            parser.add_argument("--nodes", help="comma separated list of ips of nodes")
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if pargs.e:
            esc_utils.NicePrint(self.es.cluster.put_settings('{ "transient" :{"cluster.routing.allocation.exclude._ip" : "%s" }}' % (pargs.nodes)))
            return
            
        if pargs.d:
            esc_utils.NicePrint(self.es.cluster.put_settings('{ "transient" :{"cluster.routing.allocation.exclude._ip" : "" }}'))
            return
        
    def do_clear(self, args):
        print "\033[2J"

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    try:
        readline.read_history_file("%s/.esc_history" % os.environ['HOME'])
    except IOError:
        pass
    readline.set_history_length(100) #TODO should come from config file
    prompt = ESCPrompt(sys.argv[1])
    while True:
        try: 
            prompt.cmdloop()
        except KeyboardInterrupt:
            prompt.do_quit("")
        except elasticsearch.exceptions.ConnectionError:
            print "Connection error."
            pass

