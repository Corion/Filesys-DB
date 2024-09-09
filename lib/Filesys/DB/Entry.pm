package Filesys::DB::Entry 0.01;
use 5.020;
use Moo 2;
use experimental 'signatures';
use Filesys::DB;

has [
         'entry_id'
       , 'entry_json'
       , 'last_scanned'

       , 'mountpoint'
       , 'filename'
       , 'mtime'
       , 'filesize'
       , 'sha256'
       , 'mime_type'
       , 'entry_type'
       , 'contained_by'
       , 'title'
       , 'duration'
       , 'bpm'
       , 'html'
       , 'text'
       , 'language'
       , 'thumbnail'
    ] => (
    is => 'ro',
);

sub from_row( $class, $store, $entry ) {
    return $class->new($store->_inflate_entry( $entry ));
}

sub from_id( $class, $store, $id ) {
    my $entry = $store->selectall_named( <<'SQL', $id );
        select e.entry_id
             , e.entry_json
          FROM filesystem_entry e
         where e.entry_id = $id
SQL
    $class->from_row( $store => $entry->[0] );
}

1;
