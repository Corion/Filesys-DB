#!perl
use 5.020;
use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;

our $order_by='';
our $direction='';
GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
    'config|f=s' => \my $config_file,
    'columns|c=s' => \my @columns,

    # ls options
    't' => sub { $order_by = 'mtime' },
    'r' => sub { $direction = 'desc' },
);

$order_by //= 'entry_id';

@columns = 'filename' unless @columns;
my $sql = join " ", @ARGV;

if( $order_by ) {
    $sql .= " order by $order_by $direction";
}

my $config = {};
my $user_config = {};
if(! defined $config_file ) {
    if ( 'filesys-db.yaml' ) {
        $config_file = 'filesys-db.yaml';
    } else {
        $user_config = {
            mountpoints => [
                {
                  alias => $mount_alias // '${MOUNT}',
                  directory => $mountpoint //  $ARGV[0],
                }
            ],
        }
    }
}
if( $config_file ) {
    $user_config = LoadFile( $config_file );
};
$user_config->{mountpoints} //= {};
$config->{mountpoints} = $user_config->{mountpoints};

@columns = map { split /\,/ } @columns;

my $store = Filesys::DB->new(
    mountpoints => {
        %{ $config->{mountpoints} },
        maybe $mount_alias => $mountpoint,
    },
);

print DBIx::RunSQL->format_results(
    sth => $store->entries( \@columns, $sql),
);
