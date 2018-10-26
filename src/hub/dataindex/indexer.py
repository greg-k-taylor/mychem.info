import os
import asyncio

import config
import biothings.hub.dataindex.indexer as indexer


class DrugIndexer(indexer.Indexer):

    @asyncio.coroutine
    def index(self, target_name, index_name, job_manager, steps=["index","post"],
              batch_size=10000, ids=None, mode="index"):
        # force smaller batch, as mychem has huge documents
        return super(DrugIndexer,self).index(
                target_name=target_name,
                index_name=index_name,
                job_manager=job_manager,
                steps=steps,
                batch_size=2500,
                ids=ids,
                mode=mode)
