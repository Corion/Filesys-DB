#!perl
use 5.020;
use Test2::V0 -no_srand;
use experimental 'signatures';

use DBIx::RunSQL;
use Data::Dumper;
use Filesys::DB;
use Filesys::DB::Operation;
use Filesys::DB::FTS::Tokenizer;
use File::Temp 'tempdir';
use File::Basename;

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

my $op = Filesys::DB::Operation->new(
    store => $store,
    status => sub($action,$location) {
        note( sprintf "% 8s | %s", $action, $location );
    },
    msg => sub($str) {
        diag( sprintf "%s", $str );
    },
);

$op->do_scan(
    directories => [$tempdir],
    force => 1,
);

my $documents = $store->selectall_named(<<'');
    select entry_type
         , count(*) as count
      from filesystem_entry
  group by entry_type
  order by entry_type

is $documents, [{entry_type => 'directory', count => 1},
                {entry_type => 'file', count => 1}], "We stored one document and its directory"
    or diag Dumper $documents;

my $next_id = $store->insert_or_update_direntry({ filename => "$tempdir/f1" })->{entry_id};

$documents = $store->selectall_named(<<'')->[0]->{count};
    select count(*) as count
      from filesystem_entry

is $documents, 2, "We don't store the same document twice";

$op->do_scan(
    directories => [$tempdir],
    force => 1,
);

my $new_documents = $store->selectall_named(<<'')->[0]->{count};
    select count(*) as count
      from filesystem_entry

is $new_documents, $documents, "Rescanning keeps the number of documents the same";

# Check that $id is in the directory collection for $tempdir?

my $collections = $store->selectall_named(<<'');
    select collection_type
         , title
      from filesystem_collection

is $collections, [{
        collection_type => 'directory',
        title => basename($tempdir),
    }], "We scanned one directory and created the corresponding collection"
    or diag(Dumper $collections);

done_testing();
