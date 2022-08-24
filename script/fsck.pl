#!perl
use strict;
use 5.020;

use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';

use Filesys::DB;
use Carp 'croak';
use Getopt::Long;

use DBIx::RunSQL;

my $store = Filesys::DB->new(
    #mountpoints => {
    #    $mount_alias => $mountpoint,
    #},
);

my $sth = $store->integrity_check();
$sth->execute;

print DBIx::RunSQL->format_results(
    sth => $sth,
    no_header_when_empty => 1,
);
