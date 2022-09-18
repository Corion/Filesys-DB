package Filesys::DB::Watcher;
use 5.020;

use Moo 2;
use feature 'signatures';
no warnings 'experimental::signatures';

has 'store' => (
    is => 'ro'
);

has 'mountpoints' => (
    is => 'lazy',
    default => sub($self) { [sort keys %{ $self->store->mountpoints }]},
);


=head2 C<< $_->watch %options >>

Blocks and watches all mountpoints for updates

=cut

sub watch( $self, %options ) {
    require Filesys::Notify::Win32::ReadDirectoryChanges;
    my $w = Filesys::Notify::Win32::ReadDirectoryChanges->new(subtree => 1);
    for my $mp (@{ $self->mountpoints }) {
        $w->watch_directory( path => $self->store->to_local($mp,''));
    };
    my $q = $w->queue;
    my $cb = $options{ cb };
    while( my $ev = $q->dequeue) {
        $cb->($ev);
    }
}

1;