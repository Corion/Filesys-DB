#!perl
use 5.020;
use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;

our $order_by='';
our $direction='';
our $output_format;
GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
    'config|f=s' => \my $config_file,
    'columns|c=s' => \my @columns,
    'output-format=s' => \$output_format,

    # ls options
    't' => sub { $order_by = 'mtime' },
    'r' => sub { $direction = 'desc' },
    'print0' => sub { $\ = "\0"; $output_format = 'plain' },
);

$order_by //= 'entry_id';

if( ! -t STDOUT) {
    $output_format //= 'plain';
    @columns = ('mountpoint','filename') unless @columns;
}

$output_format //= 'table';

@columns = ('mountpoint', 'filename') unless @columns;
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

if( $output_format eq 'table' ) {
    print DBIx::RunSQL->format_results(
        sth => $store->entries( \@columns, $sql),
    );
} else {
    use File::Basename;
    print dirname($_),"\0"
        for map { $store->_inflate_filename($_->{mountpoint}, $_->{filename}) }
            @{ $store->entries( \@columns, $sql)->fetchall_arrayref({}) };
}
