-- mildly inspired from
-- https://xeiaso.net/blog/sqlite-json-munge-2022-01-04

-- we really need/want DBD::SQLite 1.71_06+ for the JSON functions being
-- enabled by default

-- why not simply use an index on the function? Since SQLite will only use an
-- index on a function if it appears exactly as written in the CREATE INDEX statement
drop table if exists filesystem_entry;
create table filesystem_entry (
      entry_json    varchar(65520) not null default '{}'
    , entry_id     integer primary key not null

    , last_scanned generated always as (json_extract(entry_json, '$.last_scanned'))

      -- should we really constrain the filename to be not null? What about
      -- emails and other documents stored elsewhere? Archive files?
    , filename     unique generated always as (json_extract(entry_json, '$.filename'))
    , mtime        generated always as (json_extract(entry_json, '$.mtime'))
    , filesize     generated always as (json_extract(entry_json, '$.filesize'))
    , sha256       generated always as (json_extract(entry_json, '$.sha256'))
    , mime_type    generated always as (json_extract(entry_json, '$.mime_type'))
    , entry_type   generated always as (json_extract(entry_json, '$.entry_type')) -- 'file', 'directory', 'link' maybe?

      -- these are all unversioned in the sense that an update of the extractor
      -- mechanism won't update these
      -- also we should have metadata like "last updated" etc?
      -- or do we want all of this in another table?!
    , html         generated always as (json_extract(entry_json, '$.content.title'))
    , html         generated always as (json_extract(entry_json, '$.content.html'))
    , "text"       generated always as (json_extract(entry_json, '$.content.text'))
    , "language"   generated always as (json_extract(entry_json, '$.content.language'))
    , thumbnail    generated always as (json_extract(entry_json, '$.preview.thumbnail'))
);
create unique index idx_filesystem_entry_entry_id on filesystem_entry (entry_id);
-- We need this one so we can auto-create new rows for files
create unique index idx_filesystem_entry_filename on filesystem_entry (filename);

-- See also Audio::Directory
-- also, tags?!
drop table if exists filesystem_relation;
create table filesystem_relation (
      relation_json varchar(65520) not null default '{}'

    , relation_id       generated always as (json_extract(relation_json, '$.relation_id'))
    -- maybe, not everything corresponds to the fs

    , relation_entry_id generated always as (json_extract(relation_json, '$.entry_id'))
    , hierarchy_type    generated always as (json_extract(relation_json, '$.hierarchy_type')) -- 'directory', 'album', ???
    , title             generated always as (json_extract(relation_json, '$.title')) -- 'directory', 'album', ???
    , "position"        generated always as (json_extract(relation_json, '$.position')) -- like track number, but management is harder
);
