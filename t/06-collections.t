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

note "Generating collections";

my @generators = (
    {
        generator_id => 'creators',
        visual       => 'Creators',
        query        => <<'SQL',
            select entry_id
                , json_extract(fs.entry_json, '$.content.creator') as collection_title
            from filesystem_entry fs
            where collection_title is not null
SQL
    },

    {
        generator_id => 'languages',
        visual => 'Languages',
        query => <<'SQL',
                select entry_id
                    , json_extract(fs.entry_json, '$.language') as collection_title
                from filesystem_entry fs
                where collection_title is not null
SQL
    },
);

# maintain collections
for my $gen (@generators) {
    $op->maintain_collections(
        generator_id => $gen->{generator_id},
        query        => $gen->{query},
        visual       => $gen->{visual},
    );
}

my $collections_sizes = $store->selectall_named(<<'SQL');
    select
           c.title
         , c.generator_id
         , count(*) as "count"
      from filesystem_collection c
      join filesystem_membership m using (collection_id)
  group by c.title, c.collection_id, c.generator_id
  order by c.collection_id
SQL

note "Launching filter tests";

my $expected = [
    { title => 'Corion',   count => 2, generator_id => 'creators' },
    { title => 'A.U.Thor', count => 1, generator_id => 'creators' },
    { title => 'en',       count => 3, generator_id => 'languages' },
    { title => 'de',       count => 1, generator_id => 'languages' },
];

is $collections_sizes, $expected, "A first round creates the expected collections";
# Recreate the collections
# Check that we still have the same number of memberships

# maintain collections
for my $gen (@generators) {
    $op->maintain_collections(
        generator_id => $gen->{generator_id},
        query        => $gen->{query},
        visual       => $gen->{visual},
    );
}
is $collections_sizes, $expected, "Collection maintenance is idempotent";

# remove an item from the documents and regenerate the collections (?!)

# check that the PDF file has PDF-like properties
# at least when we have Tika installed

done_testing();
