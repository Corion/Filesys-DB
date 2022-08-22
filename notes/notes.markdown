Tika ->
    SQLite DB (by SHA256) as JSON, plus filename,size,location
    -> What does Filesys::Scanner have/provide here?!
    -> Plugins, enriching a hash?!
    -> view/virtual/calculated columns onto JSON
       calculated won't work since Add Column only works for virtual columns :/
later: on_change callback to update the DB
       store the filesystem-dependent entry ID
       also index git?! what for?

Can we / do we want to query YAML files?!
How/when do we add a full text index, and on what columns?!
