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

# ??? and now?
# Check that $id is in the directory collection for $tempdir?
