#!perl
use 5.020;
use Test2::V0 -no_srand;
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
    status => sub($action,$location, $context, $queue) {
        note sprintf "% 8s | %s", $action, $location;
    },
    msg => sub($str) {
        diag sprintf "%s", $str;
    },
);

sub add_file( $file ) {
    $file = File::Spec->rel2abs($file);
    my $context = Filesys::TreeWalker::_collect_fs_info( $file );
    my $info = {
        entry_type => 'file',
        filename => Filesys::Filename->from_native( $context->{name} ),
    };
    $info = $op->update_properties( $info, force => 1, context => $context );
}

my @files = sort glob "$base/corpus/*.markdown";
my $new_file = pop @files;
for my $file (@files) {
    add_file( $file );
};

note "Generating collections";

my @generators = (
    {
        generator_id   => 'test_creators',
        name   => 'creator',
        visual => 'Creator',
        query        => <<'SQL',
            select entry_id
                , json_extract(fs.entry_json, '$.content.creator') as collection_title
            from filesystem_entry fs
            where collection_title is not null and collection_title != ''
SQL
    },

    {
        generator_id   => 'test_languages',
        name   => 'language',
        visual => 'Language',
        query => <<'SQL',
                select entry_id
                    , json_extract(fs.entry_json, '$.language') as collection_title
                from filesystem_entry fs
                where collection_title is not null and collection_title != ''
SQL
    },
);

# maintain collections
for my $gen (@generators) {
    $op->maintain_collections(
        generator_id => $gen->{generator_id},
        query        => $gen->{query},
        visual       => $gen->{visual},
        name         => $gen->{name},
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
  order by c.title
SQL

note "Launching filter tests";

my $expected = [
    { title => 'A.U.Thor', count => 1, generator_id => 'test_creators' },
    { title => 'Corion',   count => 2, generator_id => 'test_creators' },
    { title => 'de',       count => 1, generator_id => 'test_languages' },
    { title => 'en',       count => 3, generator_id => 'test_languages' },
];

is $collections_sizes, $expected, "A first round creates the expected collections";
# Recreate the collections
# Check that we still have the same number of memberships

for my $gen (@generators) {
    $op->maintain_collections(
        generator_id => $gen->{generator_id},
        query        => $gen->{query},
        visual       => $gen->{visual},
        name         => $gen->{name},
    );
}
is $collections_sizes, $expected, "Collection maintenance is idempotent";

# add an item to the documents and regenerate the collections, see them expand
add_file( $new_file );
for my $gen (@generators) {
    $op->maintain_collections(
        generator_id => $gen->{generator_id},
        query        => $gen->{query},
        visual       => $gen->{visual},
        name         => $gen->{name},
    );
}

$collections_sizes = $store->selectall_named(<<'SQL');
    select
           c.title
         , c.generator_id
         , count(*) as "count"
      from filesystem_collection c
      join filesystem_membership m using (collection_id)
  group by c.title, c.collection_id, c.generator_id
  order by c.title
SQL

note "Launching filter tests";

$expected = [
    { title => 'A.U.Thor', count => 2, generator_id => 'test_creators' },
    { title => 'Corion',   count => 2, generator_id => 'test_creators' },
    { title => 'de',       count => 1, generator_id => 'test_languages' },
    { title => 'en',       count => 4, generator_id => 'test_languages' },
];

is $collections_sizes, $expected, "Adding a file creates the expected collections";


# remove an item from the documents and regenerate the collections
# Currently this should fail as we don't wipe the collection before recreating
# it.

done_testing();
