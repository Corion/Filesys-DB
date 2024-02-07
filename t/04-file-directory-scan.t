#!perl
use 5.020;
use Test2::V0 -no_srand;

use DBIx::RunSQL;
use Filesys::DB;
use Filesys::DB::FTS::Tokenizer;
use File::Temp 'tempdir';

my $tempdir = tempdir( CLEANUP => 1 );

my $dbh = DBIx::RunSQL->create(
    sql => 'sql/create.sql',
    dsn => 'dbi:SQLite:dbname=:memory:'
);

# Create some test temp files
{
    open my $fh, '>', "$tempdir/f1";
};

my $store = Filesys::DB->new(
    dbh => $dbh,
    mountpoints => {
        '${TEST_MOUNT}' => $tempdir,
    },
);

my $id = $store->insert_or_update_direntry({ filename => "$tempdir/test" })->{entry_id};

my $documents = $store->selectall_named(<<'')->[0]->{count};
    select count(*) as count
      from filesystem_entry

is $documents, 1, "We stored one document";

my $next_id = $store->insert_or_update_direntry({ filename => "$tempdir/test" })->{entry_id};

$documents = $store->selectall_named(<<'')->[0]->{count};
    select count(*) as count
      from filesystem_entry

is $documents, 1, "We don't store the same document twice";
is $next_id, $id, "Both times we got the same document id";

done_testing();
