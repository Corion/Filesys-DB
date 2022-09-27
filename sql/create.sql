-- mildly inspired from
-- https://xeiaso.net/blog/sqlite-json-munge-2022-01-04

-- we really need/want DBD::SQLite 1.71_06+ for the JSON functions being
-- enabled by default

-- why not simply use an index on the function? Since SQLite will only use an
-- index on a function if it appears exactly as written in the CREATE INDEX statement
-- drop table if exists filesystem_entry;
create table filesystem_entry (
      entry_json    varchar(65520) not null default '{}'
    , entry_id     integer primary key not null

    , last_scanned generated always as (json_extract(entry_json, '$.last_scanned'))

      -- should we really constrain the filename to be not null? What about
      -- emails and other documents stored elsewhere? Archive files?
    , mountpoint   generated always as (json_extract(entry_json, '$.mountpoint'))
    , filename     generated always as (json_extract(entry_json, '$.filename'))
    , mtime        generated always as (json_extract(entry_json, '$.mtime'))
    , filesize     generated always as (json_extract(entry_json, '$.filesize'))
    , sha256       generated always as (json_extract(entry_json, '$.sha256'))
    , mime_type    generated always as (json_extract(entry_json, '$.mime_type'))
    , entry_type   generated always as (json_extract(entry_json, '$.entry_type')) -- 'file', 'directory', 'link' maybe?

      -- these are all unversioned in the sense that an update of the extractor
      -- mechanism won't update these
      -- also we should have metadata like "last updated" etc?
      -- or do we want all of this in another table?!
    , title        generated always as (json_extract(entry_json, '$.content.title'))
    , duration     generated always as (json_extract(entry_json, '$.content.duration'))
    , html         generated always as (json_extract(entry_json, '$.content.html'))
    , "text"       generated always as (json_extract(entry_json, '$.content.text'))
    , "language"   generated always as (json_extract(entry_json, '$.content.language'))
    , thumbnail    generated always as (json_extract(entry_json, '$.preview.thumbnail'))
    -- add image dimensions too
);
create unique index idx_filesystem_entry_entry_id on filesystem_entry (entry_id);
-- We need this one so we can auto-create new rows for files
create unique index idx_filesystem_entry_filename on filesystem_entry (mountpoint, filename);
create index idx_filesystem_entry_filename_entry_id on filesystem_entry (mountpoint, filename, entry_id);

-- See also Audio::Directory
-- also, tags?!
-- drop table if exists filesystem_relation;
create table filesystem_relation (
      relation_json      varchar(65520) not null default '{}'

    , relation_id        generated always as (json_extract(relation_json, '$.relation_id'))
    -- maybe, not everything corresponds to the fs

    , child_id  generated always as (json_extract(relation_json, '$.child_id'))
    , parent_id generated always as (json_extract(relation_json, '$.parent_id'))
    , relation_type      generated always as (json_extract(relation_json, '$.relation_type')) -- 'directory', 'album', ???
    -- the title applies to (relation_parent_id+relation_type, not to a specific instance!
    --, title              generated always as (json_extract(relation_json, '$.title'))         -- 'directory', 'album', ???
    , "position"         generated always as (json_extract(relation_json, '$.position'))      -- like track number, but management is harder
);
create unique index idx_filesystem_relation_relation_id on filesystem_relation (relation_id);
-- We need this one so we can auto-create new rows for files
-- but this implies that an entry cannot appear twice in a playlist?!
create unique index idx_filesystem_relation_child_parent_id on filesystem_relation (relation_type,parent_id,child_id);

create table filesystem_collection (
      collection_json      varchar(65520) not null default '{}'
    , collection_id        integer primary key not null
    , collection_type      generated always as (json_extract(collection_json, '$.collection_type')) -- 'directory', 'album', ???
    , parent_id generated always as (json_extract(collection_json, '$.parent_id'))
    , title              generated always as (json_extract(collection_json, '$.title'))         
    , image              generated always as (json_extract(collection_json, '$.image'))        
);
create unique index idx_filesystem_collection_collection_id on filesystem_collection (collection_id);

create table filesystem_membership (
      membership_json      varchar(65520) not null default '{}'
    , collection_id        generated always as (json_extract(membership_json, '$.collection_id'))
    , entry_id             generated always as (json_extract(membership_json, '$.entry_id'))
    , position             generated always as (json_extract(membership_json, '$.position'))
);
create unique index idx_filesystem_membership_collection_id_entry_id on filesystem_membership (collection_id, entry_id);
-- full text search
CREATE VIRTUAL TABLE filesystem_entry_fts5
    USING fts5(content, title, "language" UNINDEXED, entry_id UNINDEXED, tokenize="perl 'Filesys::DB::FTS::Tokenizer::locale_tika_tokenizer'")
