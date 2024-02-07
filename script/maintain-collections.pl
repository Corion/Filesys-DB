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

my $op = Filesys::DB::Operation->new(
    store => $store,
    dry_run => $dry_run,
    status => sub($action,$location) {
        status( sprintf "% 8s | %s", $action, $location );
    },
    msg => sub($str) {
        msg( sprintf "%s", $str );
    },
);

if( $wipe ) {
    # Should we also delete human-touched stuff here?!
}

my @generators = LoadFile($collections);

for my $gen (@generators) {
    $op->maintain_collections(
        generator_id => $gen->{generator_id},
        query        => $gen->{query},
        visual       => $gen->{cluster_visual},
        name         => $gen->{cluster_name},
    );
}
