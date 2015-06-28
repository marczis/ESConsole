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

from cmd import Cmd
from elasticsearch import Elasticsearch
from elasticsearch import exceptions
import time
from string import atoi
#TODO get python logger for elasticsearch

PROMPT = "---->"

class ESCPrompt(Cmd):
    prompt = "ESC> "
    def __init__(self, completekey='tab', stdin=None, stdout=None):
        Cmd.__init__(self, completekey=completekey, stdin=stdin, stdout=stdout)
        self.es = Elasticsearch()

#     def do_indices_create(self, args):
#         pass
#     
#     def do_indices_delete(self, args):
#         pass

    def printAnswer(self, ans={}):
        for k,v in ans.iteritems():
            print "%-30s: %s" % (k,v)

    def do_info(self, args=""):
        """
Get basic cluster info
Args: none
"""
        self.printAnswer(self.es.info())
    
    def periodic(self, f, args):
        x = 1
        if len(args) > 0:
            x = float(args)
            
        try:
            cnt = 0
            while(True):
                print cnt
                cnt += 1
                f()
                time.sleep(x)
                print "\033[2J"
        except KeyboardInterrupt:
            return
        
    def do_periodic_cluster_health(self, args):
        """
Do ask the info over n over again.
Args: [sleep in seconds]
"""
        self.periodic(self.do_cluster_health, args)

        
    def do_cluster_get_settings(self, args):
        """
Returns cluster settings.
"""
        self.printAnswer(self.es.cluster.get_settings())
        
    def do_cluster_health(self, args =""):
        """
Provide system health
"""
        self.printAnswer(self.es.cluster.health())
    
    def do_cluster_pending_tasks(self, args=""):
        """
Ask the task list of the cluster.
"""
        self.printAnswer(self.es.cluster.pending_tasks())
        
    def do_periodic_cluster_pending_tasks(self, args):
        self.periodic(self.do_cluster_pending_tasks, args)

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
        except exceptions.NotFoundError:
            print "No hits, sorry."
            
    def do_quit(self, args):
        """Exit from ESC"""
        print "Quitting."
        raise SystemExit
    
    def do_q(self, args):
        """ Short for for quit. """
        self.do_quit(args)


if __name__ == '__main__':
    prompt = ESCPrompt()
    try: 
        prompt.cmdloop('ESConsole')
    except KeyboardInterrupt:
        prompt.do_quit("")



