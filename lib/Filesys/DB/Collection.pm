package Filesys::DB::Collection 0.01;
use 5.020;
use Moo 2;
use experimental 'signatures';
use Filesys::DB;
use Filesys::DB::Entry;
use JSON;

has [
         'collection_id'
       , 'collection_json' # should we auto-inflate this one?
       , 'image' # should we auto-inflate this one?!
       , 'title'
       , 'parent'
       , 'cluster_name'
       , 'cluster_visual'
       , 'generator_id'
       , 'generator_visual'
    ] => (
    is => 'ro',
);

has 'items' => (
    is => 'lazy',
    default => sub { $_[0]->fetch_items( $_[0]->store ) },
);

has 'store' => (
    is => 'ro',
    weak => 1,
);

sub fetch_items( $self, $store, $id=$self->collection_id ) {
    my $items = $store->selectall_named( <<'SQL', $id );
        select e.entry_id
             , e.entry_json
             , m.position as position
          FROM filesystem_collection c
          join filesystem_membership m on c.collection_id = m.collection_id
          join filesystem_entry e on m.entry_id=e.entry_id
         where c.collection_id = $id
      order by 0+m.position asc
SQL
    my %position;
    for ($items->@*) {
        $position{ $_->{entry_id}} = $_->{position};
        $_ = $store->_inflate_entry( $_ );
    }

    # Sort things, because our collections don't set up a nice default order...
    $items->@* = sort {
        my $track_a = $position{$a->{entry_id}} // $a->{content}->{track} // ($a->{filename}->native =~ /\b(\d\d)\b/);
        my $track_b = $position{$b->{entry_id}} // $b->{content}->{track} // ($b->{filename}->native =~ /\b(\d\d)\b/);
        $track_a <=> $track_b
    } $items->@*;

    return $items
}

# Yay, n+1 queries ...
sub fetch_image( $self, $store ) {
    my $image = $self->image;
    if( $image ) {
        return Filesys::DB::Entry->from_id( $store, $self->image )
    } else {
        return
    }
}

sub from_id( $class, $store, $id ) {
    my $collection = $store->selectall_named( <<'SQL', $id );
        select c.collection_id
             , c.collection_json
          FROM filesystem_collection c
         where c.collection_id = $id
SQL
    return $class->from_row( $store => $collection->[0] );
}

sub from_row( $class, $store, $row ) {
    my $payload = decode_json( $row->{collection_json});
    return $class->new({ collection_id => $row->{collection_id}, store => $store, $payload->%* });
}


1;
