#!perl
use 5.020;
use Filesys::DB;
use Filesys::DB::Entry;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;
use Encode 'encode', 'decode';

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

@columns = ('entry_id', 'entry_json', 'filename' );

my $store = Filesys::DB->new(
    mountpoints => {
        %{ $config->{mountpoints} },
        maybe $mount_alias => $mountpoint,
    },
);

my $sth = $store->entries( \@columns, $sql );

my @entries = map {
    Filesys::DB::Entry->from_row( $store, $_ )
} $sth->fetchall_arrayref( {} )->@*;
# Check that the files still exist:
for my $e (@entries) {
    my $fn_bytes = $e->filename->native;
    if( ! -e $fn_bytes ) {
        # The file has gone missing, delete it from the DB
        say encode('UTF-8', $e->filename->value );
    } else {
        # Should we do a rescan here? That's what the --rescan option
        # in scan-filesystem is for.
    };
}
