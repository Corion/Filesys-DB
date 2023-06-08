#!perl
use 5.020;
use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;

use Filesys::DB::FTS::Tokenizer;

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

# Kill off all indices
my $dbh_index = $dbh->selectall_arrayref(<<'', { Slice => {}});
    SELECT name FROM sqlite_master
     WHERE type == 'index'

for my $index (@$dbh_index) {
    $dbh->do("drop index $index->{name}");
}
#$dbh->do("drop table filesystem_entry_fts5");

#$dbh->do(<<'SQL');
#    alter table filesystem_entry
#    rename to filesystem_entry_old
#SQL

$dbh->do("drop table filesystem_entry");
$dbh->do("drop table filesystem_relation");

$dbh = DBIx::RunSQL->create( sql => 'sql/create.sql', dbh => $dbh );

# This assumes that the order of columns doesn't change
$dbh->do(<<SQL);
    insert into filesystem_entry(entry_id, entry_json)
    select entry_id
         , entry_json
      from filesystem_entry_old
SQL
