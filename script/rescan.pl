#!perl
use 5.020;
use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
    'config|f=s' => \my $config_file,
    'columns|c=s' => \my @columns,
);

@columns = 'filename' unless @columns;
my $sql = join " ", @ARGV;

my $config = {};
my $user_config = {};
if(! defined $config_file ) {
    if ( -f 'filesys-db.yaml' ) {
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

my $sth = $store->entries( \@columns, $sql );

