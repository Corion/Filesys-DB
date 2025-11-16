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
    , mountpoint   generated always as (json_extract(entry_json, '$.mountpoint')) stored
    , filename     generated always as (json_extract(entry_json, '$.filename'))   stored
    , mtime        generated always as (json_extract(entry_json, '$.mtime'))      stored
    , filesize     generated always as (json_extract(entry_json, '$.filesize'))   stored
    , sha256       generated always as (json_extract(entry_json, '$.sha256'))     stored
    , mime_type    generated always as (json_extract(entry_json, '$.mime_type'))  stored
    , entry_type   generated always as (json_extract(entry_json, '$.entry_type')) stored

    -- PDF files contain images, archive files contain other files
    , is_container generated always as (json_extract(entry_json, '$.is_container')) stored
	-- the entry_id of the container file?!
	-- or do we want this as a separate table?!
	-- the filename will be the name of the container, with the internal resource name added?!
    , contained_by generated always as (json_extract(entry_json, '$.contained_by')) stored
	-- do we (optionally) store a copy of the content in a "blobs" table?!
	-- possibly indexed by the MD5 instead of the entry id?!

      -- these are all unversioned in the sense that an update of the extractor
      -- mechanism won't update these
      -- also we should have metadata like "last updated" etc?
      -- or do we want all of this in another table?!
    , title        generated always as (json_extract(entry_json, '$.content.title'))     stored
    , artist       generated always as (json_extract(entry_json, '$.content.artist'))    stored
    , duration     generated always as (json_extract(entry_json, '$.content.duration'))  stored
    , bpm          generated always as (json_extract(entry_json, '$.content.bpm'))       stored
    , html         generated always as (json_extract(entry_json, '$.content.html'))      stored
    , "text"       generated always as (json_extract(entry_json, '$.content.text'))      stored
    , "language"   generated always as (json_extract(entry_json, '$.content.language'))  stored
    , thumbnail    generated always as (json_extract(entry_json, '$.preview.thumbnail')) stored
    -- add image dimensions too
);
create unique index idx_filesystem_entry_entry_id on filesystem_entry (entry_id);
-- We need this one so we can auto-create new rows for files
create unique index idx_filesystem_entry_filename on filesystem_entry (mountpoint, filename);
create index idx_filesystem_entry_filename_entry_id on filesystem_entry (mountpoint, filename, entry_id);
create index idx_filesystem_entry_last_modified on filesystem_entry (entry_id, mime_type, mtime);

/* The filesystem_collection is an arbitrary collection */
create table filesystem_collection (
      collection_json        varchar(65520) not null default '{}'
    , collection_id          integer primary key not null
    , collection_type        generated always as (json_extract(collection_json, '$.collection_type')) -- 'directory', 'album', 'documents', ???
    , collection_type_visual generated always as (json_extract(collection_json, '$.collection_type_visual')) -- 'Directory' ???
    , parent_id              generated always as (json_extract(collection_json, '$.parent_id'))
    , title                  generated always as (json_extract(collection_json, '$.title'))
    , image                  generated always as (json_extract(collection_json, '$.image'))

    -- we want another collection type thing, that says "Language", "Genre", or whatever...
    , cluster_name           generated always as (json_extract(collection_json, '$.cluster_name'))
    , cluster_visual         generated always as (json_extract(collection_json, '$.cluster_visual'))

    -- generator is for automatic category creation so a generator can wipe/recreate
    -- its stuff
    , generator_id           generated always as (json_extract(collection_json, '$.generator_id'))
    , generator_visual       generated always as (json_extract(collection_json, '$.generator_visual'))
);
create unique index idx_filesystem_collection_collection_id on filesystem_collection (collection_id);
create unique index idx_filesystem_collection_directory_parent_id on filesystem_collection (collection_type,parent_id);

create table filesystem_membership (
      membership_json      varchar(65520) not null default '{}'
    , membership_id        integer primary key not null
    , collection_id        generated always as (json_extract(membership_json, '$.collection_id'))
    , entry_id             generated always as (json_extract(membership_json, '$.entry_id'))
    , position             generated always as (json_extract(membership_json, '$.position'))
    , generator_id         generated always as (json_extract(membership_json, '$.generator_id')) -- 'manual' or 'id'
    -- this won't handle manual _ex_clusions, but good enough for the time being
);
create unique index idx_filesystem_membership_collection_id_entry_id on filesystem_membership (collection_id, entry_id);

-- full text search
CREATE VIRTUAL TABLE filesystem_entry_fts5
    USING fts5(
        html
      , title
      , "language" UNINDEXED
      , entry_id UNINDEXED
      , tokenize="perl 'Filesys::DB::FTS::Tokenizer::locale_tika_tokenizer'"
);

-- Triggers to keep the FTS index up to date.
DROP TRIGGER IF EXISTS filesystem_entry_ai;
CREATE TRIGGER filesystem_entry_ai AFTER INSERT ON filesystem_entry BEGIN
  INSERT INTO filesystem_entry_fts5(html, title, "language", entry_id) VALUES (new.html, new.title, new."language", new.entry_id); --
END;

--CREATE TRIGGER filesystem_entry_ad AFTER DELETE ON filesystem_entry BEGIN
--  INSERT INTO filesystem_entry_fts5(filesystem_entry_fts5, html, title, "language", entry_id) VALUES('delete', old.html, old.title, old."language", old.entry_id);
--END;

DROP TRIGGER IF EXISTS filesystem_entry_au;
CREATE TRIGGER filesystem_entry_au AFTER UPDATE ON filesystem_entry BEGIN
  --INSERT INTO filesystem_entry_fts5(filesystem_entry_fts5, html, title, "language", entry_id) VALUES('delete', old.html, old.title, old."language", old.entry_id);
  INSERT INTO filesystem_entry_fts5(html, title, "language", entry_id) VALUES (new.html, new.title, new."language", new.entry_id);
END;
