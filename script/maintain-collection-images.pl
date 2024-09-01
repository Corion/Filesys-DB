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
use Filesys::DB::TermIO 'status', 'msg';

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
    'config|c=s'     => \my $config_file,
    'dsn|d=s'        => \my $dsn,
    'rescan|r'       => \my $rescan,
    'dry-run|n'      => \my $dry_run,
);

$dsn //= 'dbi:SQLite:dbname=db/filesys-db.sqlite';

my $store = Filesys::DB->new(
   dbh => {
       dsn => $dsn,
   }
);
$store->init_config(
    default_config_file => 'filesys-db.yaml',
    config_file         => $config_file,
);

my $op = Filesys::DB::Operation->new(
    store => $store,
    dry_run => $dry_run,
    status => \&status,
    msg    => \&msg,
);

$op->maintain_collection_images();
