#!perl
use strict;
use 5.020;

#use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';
use PerlX::Maybe;

use Filesys::DB;
use Filesys::DB::Watcher;
use Filesys::DB::Operation;

use Carp 'croak';
use Getopt::Long;
use POSIX 'strftime';
use Encode 'encode', 'decode';

use JSON::Path;
use YAML 'LoadFile';

use File::Basename;

use Filesys::DB::FTS::Tokenizer; # so that the tokenizer subroutine is found

BEGIN {
$ENV{HOME} //= $ENV{USERPROFILE}; # to shut up Music::Tag complaining about undefined $ENV{HOME}
}

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s'      => \my $mount_alias,
    'config|c=s'     => \my $config_file,
    'dsn|d=s'        => \my $dsn,
    'rescan|r'       => \my $rescan,
    'dry-run|n'      => \my $dry_run,
    'collections=s'  => \my $collections,
    'wipe'           => \my $wipe,
);

$dsn //= 'dbi:SQLite:dbname=db/filesys-db.sqlite';
$collections //= 'collections.yaml';

my $store = Filesys::DB->new(
   dbh => {
       dsn => $dsn,
   }
);
$store->init_config(
    default_config_file => 'filesys-db.yaml',
    config_file         => $config_file,
);

if( $mount_alias and $mountpoint ) {
    $store->mountpoints->{ $mount_alias } = +{ directory => $mountpoint, alias => $mount_alias };
}

if( $wipe ) {
    # Should we also delete human-touched stuff here?!
}

my @generators = LoadFile($collections);

for my $gen (@generators) {
    # find/wipe all collections created by this which were not touched
    # by a human:

    my $generator_id = $gen->{generator_id};

    my $touched = $store->selectall_named(<<'', $generator_id);
    with generated as (
       select fm.collection_id
         from filesystem_membership fm
        where fm.generator_id = :generator_id
    )
    , touched as (
       select distinct fm.collection_id
         from filesystem_membership fm
         join generated g on fm.collection_id = g.collection_id
        where fm.generator_id is null
           or fm.generator_id != :generator_id
    )
    select * from touched

    my $generated = $store->selectall_named(<<'', $generator_id);
    with generated as (
       select collection_id
         from filesystem_membership fm
        where fm.generator_id = :generator_id
    )
    select * from generated

    # Find the set of collections that the queries describe:
    my $collections = $store->selectall_named( $gen->{query} );

    my %collections;

    for my $rel ( $collections->@* ) {
        # Update the connection via name and generator id:
        my $collection_title = $rel->{collection_title};
        $collection_title = decode('UTF-8', $collection_title);
        next unless defined $collection_title; # NULLs don't get added

        if( ! $collections{ $collection_title }) {
            my $exists = $store->selectall_named(<<'', $collection_title, $generator_id );
                select collection_id
                     , collection_json
                  from filesystem_collection
                 where generator_id = :generator_id
                   and title = :collection_title

            if( ! $exists->@* ) {
                # create the collection
                say sprintf "%s: Creating '%s'", $generator_id, $collection_title;
                $collections{ $collection_title } = $store->insert_or_update_collection({
                    generator_id => $generator_id,
                    title => $collection_title,
                    generator_visual => $gen->{visual},
                    # well, this should come from the query, no?!
                    collection_type => 'documents',
                });

            } else {
                say sprintf "%s: Have '%s'", $generator_id, $collection_title;
                $collections{ $collection_title } = $store->find_collection( $exists->[0]->{collection_id});
            }
        }

        # Wipe existing membership, if it is different
        # Create new membership
        $store->insert_or_update_membership({
            collection_id => $collections{ $collection_title }->{collection_id},
            entry_id => 0+$rel->{entry_id},
            generator_id => $generator_id, # well, we shouldn't clobber the manual relations...
            # Position also from $rel, if it exists
        });

    }

    # Check collections that were created by us some time,
    # but don't exist anymore. Delete these if they were not touched by
    # a human hand

}
