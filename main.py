#!/usr/bin/python

# from datetime import datetime
# from elasticsearch import Elasticsearch
# es = Elasticsearch()
#  
# doc = {
#     'author': 'kimchy',
#     'text': 'Elasticsearch: cool. bonsai cool.',
#     'timestamp': datetime(2010, 10, 10, 10, 10, 10)
# }
# res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
# print(res['created'])
#  
# res = es.get(index="test-index", doc_type='tweet', id=1)
# print(res['_source'])
#  
# es.indices.refresh(index="test-index")
#  
# res = es.search(index="test-index", body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

import cmd
import elasticsearch
import sys
import argparse

import esc_utils


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

#     def do_indices_create(self, args):
#         pass
#     
#     def do_indices_delete(self, args):
#         pass


    def do_info(self, args=""):
        """
Get basic cluster info
Args: none
"""
        esc_utils.niceprint(self.es.info())
    
    
        
    def do_cluster_get_settings(self, args):
        """
Returns cluster settings.
"""
        esc_utils.niceprint(self.es.cluster.get_settings())

    def cluster_health(self):
        esc_utils.niceprint(self.es.cluster.health())
        
    def do_cluster_health(self, args =""):
        """
Provide system health
"""
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
        
        
    
    def cluster_pending_tasks(self):
        
        esc_utils.niceprint(self.es.cluster.pending_tasks())
        
    def do_cluster_pending_tasks(self, args=""):
        """
Ask the task list of the cluster.
"""
        try:
            parser = argparse.ArgumentParser(prog="cluster_pending_tasks")
            parser.add_argument("-c", action="store_true", help="Do periodic query, use -t to set sleep time")
            parser.add_argument("-t", type=int, help="seconds between two query, use with -c", default=1)
            pargs = parser.parse_args(esc_utils.arrayArgs(args))
        except:
            return
        
        if pargs.c:
            esc_utils.periodic(self.cluster_pending_tasks, pargs.t)
            return
        
        self.cluster_pending_tasks()
   

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
                if (filter(None, line.split(" "))[3] == pargs.state.upper()): #Magic Do not touch
                    print line
                    
        
    def do_search_index(self, args):
        """
Search the elasticsearch for a given index name 
Args: <pattern>
"""
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
        """Exit from ESC"""
        print "Quitting."
        raise SystemExit
    
    def do_q(self, args):
        """ Short for for quit. """
        self.do_quit(args)


if __name__ == '__main__':
    prompt = ESCPrompt(sys.argv[1:])
    try: 
        prompt.cmdloop('ESConsole')
    except KeyboardInterrupt:
        prompt.do_quit("")

