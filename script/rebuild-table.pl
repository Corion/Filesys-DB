#!perl
use 5.020;
use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;

GetOptions(
    'config|f=s' => \my $config_file,
);

my $config = {};
my $user_config = {};
if(! defined $config_file ) {
    if ( 'filesys-db.yaml' ) {
        $config_file = 'filesys-db.yaml';
    }
}
if( $config_file ) {
    $user_config = LoadFile( $config_file );
};
$user_config->{mountpoints} //= {};
$config->{mountpoints} = $user_config->{mountpoints};

my $store = Filesys::DB->new(
    mountpoints => {},
);

my $dbh = $store->dbh;

#$dbh->do(<<'SQL');
#    alter table filesystem_entry
#    rename to filesystem_entry_old
#SQL
#$dbh->do(<<'SQL');
#    drop index idx_filesystem_entry_entry_id
#SQL
#
#$dbh = DBIx::RunSQL->create( sql => 'sql/create.sql', dbh => $dbh );

# This assumes that the order of columns doesn't change
$dbh->do(<<SQL);
    insert into filesystem_entry(entry_id, entry_json)
    select entry_id
         , entry_json
      from filesystem_entry_old
SQL
