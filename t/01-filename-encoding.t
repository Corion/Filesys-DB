#!perl
use 5.020;
use Test::More tests => 3;

use DBIx::RunSQL;
use Filesys::DB;

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

my $filename_octets = "\x{ef}\x{bb}\x{bf}House Gospel Choir";

my $id = $store->insert_or_update_direntry({ filename => $filename_octets })->{entry_id};

# my $sth = $dbh->prepare('select * from filesystem_entry');
# $sth->execute;
# note( DBIx::RunSQL->format_results(sth => $sth));

my $info = $store->find_direntry_by_filename($filename_octets);
ok $info, "We can find an existing filename with UTF-8 in its parts";
is $info->{filename}, $filename_octets, "We get the same filename octets back";
my $reinserted = $store->insert_or_update_direntry({ filename => $info->{filename} })->{entry_id};
if(! is $reinserted, $id, "We detect duplicates even when decoding from the filename") {
    my $sth = $dbh->prepare('select * from filesystem_entry');
    $sth->execute;
    note( DBIx::RunSQL->format_results(sth => $sth));
};
