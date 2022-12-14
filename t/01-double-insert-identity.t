#!perl
use 5.020;
use Test::More tests => 2;

use DBIx::RunSQL;
use Filesys::DB;
use Filesys::DB::FTS::Tokenizer;

my $dbh = DBIx::RunSQL->create(
    sql => 'sql/create.sql',
    dsn => 'dbi:SQLite:dbname=:memory:'
);

my $store = Filesys::DB->new(
    dbh => $dbh,
    mountpoints => {
        '${TEST_MOUNT}' => '',
    },
);

my $id = $store->insert_or_update_direntry({ filename => 'test' })->{entry_id};

my $new_id = $store->find_direntry_by_filename('test')->{entry_id};
if(! is $id, $new_id, "We can find an existing filename") {
    my $sth = $dbh->prepare('select * from filesystem_entry');
    $sth->execute;
    note( DBIx::RunSQL->format_results(sth => $sth));
};
my $reinserted = $store->insert_or_update_direntry({ filename => 'test' })->{entry_id};
is $reinserted, $id, "We detect duplicates";
