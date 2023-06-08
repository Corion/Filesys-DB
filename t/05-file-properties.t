#!perl
use 5.020;
use Test::More tests => 2;

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
# take a PDF file from the corpus
{
    open my $fh, '>', "$tempdir/f1";
};

my $store = Filesys::DB->new(
    dbh => $dbh,
    mountpoints => {
        '${TEST_MOUNT}' => $tempdir,
    },
);

my $id = $store->insert_or_update_direntry({ filename => 'test' })->{entry_id};

# check that the PDF file has PDF-like properties
# at least when we have Tika installed
