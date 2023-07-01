#!perl
use 5.020;
use Test2::V0;
use feature 'signatures';
no warnings 'experimental::signatures';

use DBIx::RunSQL;
use Filesys::DB;
use Filesys::DB::Operation;
use Filesys::DB::FTS::Tokenizer;
use File::Temp 'tempdir';
use Filesys::TreeWalker;
use File::Spec;
use File::Basename 'dirname';

my $tempdir = tempdir( CLEANUP => 1 );

my $dbh = DBIx::RunSQL->create(
    sql => 'sql/create.sql',
    dsn => 'dbi:SQLite:dbname=:memory:'
);

my $base = File::Spec->rel2abs( dirname($0) . '/..' );

my $store = Filesys::DB->new(
    dbh => $dbh,
    mountpoints => {
        '${TEST_MOUNT}' => "$base/corpus",
    },
);

my $op = Filesys::DB::Operation->new(
    store => $store,
    status => sub($action,$location) {
        note sprintf "% 8s | %s", $action, $location;
    },
    msg => sub($str) {
        diag sprintf "%s", $str;
    },
);

# Create some test temp files
# take a PDF file from the corpus - we should have something better,
# especially, we don't want PDFs as they are slow to read. Maybe we want to
# simply index markdown files?!!
for my $file (glob "$base/corpus/*.markdown") {
    $file = File::Spec->rel2abs($file);
    my $context = Filesys::TreeWalker::_collect_fs_info( $file );
    my $info = {
        entry_type => 'file',
        filename => Filesys::Filename->from_native( $context->{name} ),
    };
    $info = $op->update_properties( $info, force => 1, context => $context );
};

note "Launching filter tests";

# maintain collections

# check that the PDF file has PDF-like properties
# at least when we have Tika installed

done_testing();
