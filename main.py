#!/usr/bin/python

import cmd
import elasticsearch
import sys
import argparse
import re

import esc_utils
from test.test_support import args_from_interpreter_flags

#TODO get python logger for elasticsearch
#Ideas:
# - SSH tunnel for remote connections

class ESCPrompt(cmd.Cmd):
    prompt = "ESC> "
    def __init__(self, hosts=["localhost:9200"], completekey='tab', stdin=None, stdout=None):
        cmd.Cmd.__init__(self, completekey=completekey, stdin=stdin, stdout=stdout)
        self.hosts = hosts
        if not self.hosts:
            self.hosts = ["localhost:9200"]
        self.es = elasticsearch.Elasticsearch(self.hosts)
        ESCPrompt.prompt = "ESC %s > " % (self.hosts)

    def do_info(self, args=""):
        """ Get basic cluster info """
        esc_utils.NicePrint(self.es.info())

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
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if pargs.state == "all":
            print self.es.cat.shards()
        else:
            for line in self.es.cat.shards().splitlines(): 
                if re.match(pargs.state.upper(), filter(None, line.split(" "))[3]): #Magic Do not touch
                    print line

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
        raise SystemExit

    def do_q(self, args):
        """ Shorty for for quit. """
        self.do_quit(args)


if __name__ == '__main__':
    prompt = ESCPrompt(sys.argv[1:])
    try: 
        prompt.cmdloop('Welcome to the ESConsole !')
    except KeyboardInterrupt:
        prompt.do_quit("")

