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
        note( sprintf "% 10s | %s", $action, $location );
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

my $filename = "$tempdir/f1";
my $info = $op->basic_direntry_info("$tempdir/f1", "$tempdir/f1", { stat => [stat $filename] });
my $info2 = $store->insert_or_update_direntry($info);
my $next_id = $info2->{entry_id};
my @props = Filesys::DB::Operation::_applicable_properties( \%Filesys::DB::Operation::file_properties, $info, {} );

is \@props, [['mime_type', $Filesys::DB::Operation::file_properties{'$.mime_type'}],
             ['sha256',    $Filesys::DB::Operation::file_properties{'$.sha256'}],
            ], "We want to add a mime type and sha256 on rescanning"
    or diag Dumper \@props;

# If we don't have the mime type, we can't find the other properties...

my $info3 = $op->update_properties( $info2, force => 1, );
$info3->{filename}->{value} = $filename;
ok exists $info3->{ sha256 }, "We have a sha256 now"
    or diag Dumper $info3;
ok exists $info3->{ mime_type }, "We have a MIME type now"
    or diag Dumper $info3;
ok exists $info3->{ content }, "We have a content tree now"
    or diag Dumper $info3;
ok exists $info3->{ content }->{ creator }, "We have a content.creator now"
    or diag Dumper $info3;
is $info3->{ content }->{ creator }, Filesys::DB::Operation::EXISTS_BUT_EMPTY, "... and it is our 'exists but empty' marker"
    or diag Dumper $info3;

my @props3 = map { $_->[0] }
             Filesys::DB::Operation::_applicable_properties( \%Filesys::DB::Operation::file_properties, $info3, {} );
is \@props3, [], "Existing but undef properties don't get rescanned"
    or diag Dumper $info3;

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

# Do a second scan and check that we don't read the whole file for
# re-determining the properties as long as size and mtime are the same
# ... and we already have all potential properties

$info = $op->basic_direntry_info( "$tempdir/f1", "$tempdir/f1", { stat => [stat("$tempdir/f1")] }, { entry_type => 'file' } );
is $op->_wants_rescan( $info, {} ), undef, "We don't want to rescan $info->{filename}"
    or diag Dumper $info;

my $fs_info = { stat => [stat("$tempdir/f1")] };
$info = $op->basic_direntry_info( "$tempdir/f1", "$tempdir/f1", $fs_info, { entry_type => 'file' } );
$fs_info->{stat}->[7] = 10; # set the filesize
is $op->_wants_rescan( $info, { context => $fs_info } ), 1, "A filesize change means we will rescan"
    or diag Dumper $info;

$fs_info = { stat => [stat("$tempdir/f1")] };
$info = $op->basic_direntry_info( "$tempdir/f1", "$tempdir/f1", $fs_info, { entry_type => 'file' } );
$fs_info->{stat}->[9] = time(); # Update the mtime field
is $op->_wants_rescan( $info, { context => $fs_info } ), 1, "An mtime means we will rescan"
    or diag Dumper $info;

# XXX set up scan access counter
my %properties_read;

$op->do_scan(
    directories => [$tempdir],
);

is $properties_read{ $filename }->{ sha256 }, undef, "We didn't recompute the sha256 for $filename";

done_testing();
