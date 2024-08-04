#!perl
use 5.020;
use experimental 'signatures';

use Test2::V0 '-no_srand';

use DBIx::RunSQL;
use Filesys::DB;
use Filesys::DB::Operation;
use Filesys::DB::FTS::Tokenizer;
use File::Temp 'tempdir';
use POSIX 'strftime';

plan tests => 2;

my $tempdir = tempdir( CLEANUP => 1 );
my $fn = 'test-2.markdown';

my $dbh = DBIx::RunSQL->create(
    sql => 'sql/create.sql',
    dsn => 'dbi:SQLite:dbname=:memory:'
);

# Create some test temp files
# take a PDF file from the corpus
{
    use File::Copy 'cp';
    cp "corpus/$fn" => $tempdir;
};

my $store = Filesys::DB->new(
    dbh => $dbh,
    mountpoints => {
        '${TEST_MOUNT}' => $tempdir,
    },
);

my $info = $store->insert_or_update_direntry({ filename => "$tempdir/$fn", entry_type => 'file' });
my $id = $info->{entry_id};

# check that the PDF file has PDF-like properties
# at least when we have Tika installed
ok $id, "We inserted the file";

#use Data::Dumper;
#diag Dumper $info;

note "Fetching file information";
my $op = Filesys::DB::Operation->new(
    store => $store,
    #dry_run => $dry_run,
    status => sub($action,$location, $context, $queue) {
        #my $date = strftime '%Y-%m-%d', gmtime( $context->{stat}->[9] );
        #status( sprintf "% 10s | %s | % 6d | %s", $action, $date, scalar( @$queue ), $location );
    },
    msg => sub($str) {
        note( sprintf "%s", $str );
    },
);
$info = $op->update_properties( $info );

is $info->{mime_type}, 'text/plain', "We detect a text/plain type (for markdown with frontmatter...)";
exists $info->{last_scanned}, "We marked the file as recently scanned";
done_testing;
