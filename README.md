# XML stream generator for Sphinx search engine.
If you need to index data from Cassandra database using Sphinx xmlpipe2 data source, this project might be useful.
It is generating XML specified by [Sphinx documentation](http://sphinxsearch.com/docs/current.html#xmlpipe2).
And can generate keys for Sphinx if you have no suitable int or bigint primary keys in Cassandra tables or use UUID.
You can download [compiled JAR](https://github.com/Denis-Mak/cql-xmlpipe/releases/download/1.0-SNAPSHOT/cql-xmlpipe.1.0-SNAPSHOT.tar.gz)
and use command line shell script (as in example) or modify source code as you need.

**Usage example**
    `query -keys url,pos -cql "SELECT content, title, url, pos FROM test.Pages"`

**Requirements**
   Cassandra version > 2.0
   CQL supported

