# XML stream generator for Sphinx search engine.
If you need to index data from Cassandra database using Sphinx xmlpipe2 data source, this project might be useful.
It is generating XML specified by [Sphinx documentation](http://sphinxsearch.com/docs/current.html#xmlpipe2).
And can generate keys for Sphinx if you have no suitable int or bigint primary keys in Cassandra tables or use UUID.
You can download [compiled JAR](https://github.com/Denis-Mak/cql-xmlpipe/releases/download/1.0/cql-xmlpipe-1.0.tar.gz)
and use command line shell script (as in example) or modify source code as you need.

**Usage example**
    `query -keys url,pos -cql "SELECT content, title, url, pos FROM test.Pages"`

**Sphinx config example**
    ```
    source src_test
    {
    type                    = xmlpipe2
    xmlpipe_command         = /usr/local/cql-xmlpipe/query -host 192.168.1.101 -user cassandra_user -pass cassandra_user_password -keys url, pos \
                                    -cql "SELECT content, title, url, pos FROM test.Pages
    xmlpipe_field           = content
    xmlpipe_field           = title
    xmlpipe_attr_string     = url
    xmlpipe_attr_uint       = pos
    xmlpipe_fixup_utf8      = 1
    }
    ```

**Requirements**
   Cassandra version > 2.0
   CQL supported

