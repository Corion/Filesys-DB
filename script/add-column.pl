#!perl
use strict;
use 5.020;
use DBI;

my $dbh = DBI->connect('dbi:SQLite:dbname=db/filesys-db.sqlite', undef, undef, { RaiseError => 1, PrintError => 0 });

# Remember to also update sql/create.sql :)

my ($column, $json_path) = @ARGV;
$dbh->do(<<"SQL");
    alter table filesystem_entry add "$column" generated always as (json_extract(entry_json, '$json_path'))
SQL
