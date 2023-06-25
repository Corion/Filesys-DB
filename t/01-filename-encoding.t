#!perl
use 5.020;
use Test::More tests => 5;

use DBIx::RunSQL;
use Filesys::DB;
use Filesys::DB::FTS::Tokenizer;
use Encode 'is_utf8';

my $dbh = DBIx::RunSQL->create(
    sql => 'sql/create.sql',
    dsn => 'dbi:SQLite:dbname=:memory:'
);

my $store = Filesys::DB->new(
    dbh => $dbh,
    mountpoints => {
        '${TEST_MOUNT}' => 'y',
    },
);

# A filename with a BOM ?!
my $filename_octets = "y/\x{ef}\x{bb}\x{bf}House Gospel Choir";

my $stored = $store->insert_or_update_direntry({ filename => $filename_octets });
if(! ok is_utf8( $stored->{filename}->value), "The stored filename is Unicode" ) {
    # Well, maybe it just was not Unicode but ASCII ...
    diag $stored->{filename}->value;
}
my $id = $stored->{entry_id};

# my $sth = $dbh->prepare('select * from filesystem_entry');
# $sth->execute;
# note( DBIx::RunSQL->format_results(sth => $sth));

my $info = $store->find_direntry_by_filename($filename_octets);
ok $info, "We can find an existing filename with UTF-8 in its parts";

ok is_utf8( $info->{filename}->value), "The returned filename is Unicode";

is $info->{filename}->value, $store->decode_filename($filename_octets)->value, "We get a Unicode filename back";

my $reinserted = $store->insert_or_update_direntry({ filename => $info->{filename} })->{entry_id};
if(! is $reinserted, $id, "We detect duplicates even when decoding from the filename") {
    my $sth = $dbh->prepare('select * from filesystem_entry');
    $sth->execute;
    note( DBIx::RunSQL->format_results(sth => $sth));
};
