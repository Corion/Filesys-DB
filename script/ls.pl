#!perl
use 5.020;
use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
);

my $sql = join " ", @ARGV;

$mount_alias //= '${MOUNT}';
$mountpoint //= $ARGV[0];


my $store =Filesys::DB->new(
    mountpoints => {
        $mount_alias => $mountpoint,
    },
);
print DBIx::RunSQL->format_results(
    sth => $store->entries( $sql ),
);
