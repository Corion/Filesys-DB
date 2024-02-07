#!perl
use 5.020;
use experimental 'signatures';
use PerlX::Maybe;
use Filesys::DB;
use Getopt::Long;
use YAML 'LoadFile';

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
    'config|f=s' => \my $config_file,
    'create|c' => \my $create, # create the collection if not found
    'type=s'   => \my $collection_type,
);

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

my $store = Filesys::DB->new(
    mountpoints => {
        %{ $config->{mountpoints} },
        maybe $mount_alias => $mountpoint,
    },
);

my ($collection_title, @items) = @ARGV;

sub create_collection( $store, $title ) {
    return $store->insert_or_update_collection({
        generator_id => undef,
        title => $title,
        # well, this should come from the query, no?!
        collection_type => 'documents',
    });
}

sub find_collection( $store, $title ) {
    my $exists = $store->selectall_named(<<'', $collection_title );
        select collection_id
             , collection_json
          from filesystem_collection
         where title = :collection_title

    if( ! $exists->@* ) {
        # create? or complain?!
        if( $create ) {
            return create_collection( $store, $title );
        } else {
            return undef
        }

    } elsif( $exists->@* > 1 ) {
        # complain
        say "Multiple collections found:";
        for ( $exists->@* ) {
            use Data::Dumper;
            say Dumper $store->find_collection( $_->{collection_id});
        }


    } else {
        return $store->find_collection( $exists->[0]->{collection_id})
    }
}

sub insert_item( $collection, $item ) {

        # Create new membership
        $store->insert_or_update_membership({
            collection_id => $collection->{collection_id},
            entry_id => 0+$item,
            generator_id => 'manual',
            # Position also from $rel, if it exists
        });
}

my $collection = find_collection( $store, $collection_title );

# Hrrr - we always create the collection if it didn't exist beforehand ...
if( ! $collection ) {
    die "No collection found for '$collection_title'";
}

for my $item (@items) {
    insert_item( $collection, $item );
}
