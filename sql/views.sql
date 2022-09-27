create view latest_albums as
select
       c.collection_id
     , c.title
     , max(e.mtime) as last_update
  FROM filesystem_membership m
  join filesystem_collection c on m.collection_id=c.collection_id
  join filesystem_entry e on m.entry_id=e.entry_id
 where e.mime_type like 'audio/%'
 group by c.title, c.parent_id
 order by last_update desc
 limit 20 offset 0
