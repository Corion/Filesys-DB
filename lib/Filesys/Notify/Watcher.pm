package Filesys::Notify::Watcher;
use 5.020;
use File::ChangeNotify;

use feature 'signatures';
no warnings 'experimental::signatures';

use threads;
use Thread::Queue;

=head2 C<< ->new %options >>

  my $w = Filesys::Notify::Win32::ReadDirectoryChanges->new(
      directories => \@ARGV,
      subtree => 1,
  );

=cut

sub BUILD($self, $args) {
    if( my $dirs = delete $args->{directory}) {
        $dirs = [$dirs] if ! ref $dirs;
        for my $d (@$dirs) {
            $self->watch_directory( path => $d );
        }
    }
}

has 'subtree' => (
    is => 'ro',
);

has 'watchers' => (
    is => 'lazy',
    default => sub{ +{} },
);

=head2 C<< ->queue >>

  my $q = $w->queue;

Returns the L<Thread::Queue> object where the filesystem events get
passed in. Use this for integration with your own event loop.

=cut

has 'queue' => (
    is => 'lazy',
    default => sub { Thread::Queue->new() },
);


sub _watcher($path,$subtree,$queue) {
    my $running = 1;
    my $w = File::ChangeNotify->instantiate_watcher(directories => [$path]);
    state %translate = (
        create => 'added',
        modify => 'modified',
        delete => 'removed',
        unknown => 'unknown',
    );
local $SIG{KILL}=sub{threads->exit};
    while($running) {
        my @events = $w->wait_for_events;

        for my $e (@events) {
            my $i = {
                path   => $ev->path,
                action => $translate{ $ev->type },
            };
            $queue->enqueue($i);
        };
    }
};

sub build_watcher( $self, %options ) {
    my $path = delete $options{ path };
    my $subtree = !!( $options{ subtree } // $self->subtree );
    my $queue = $self->queue;
    $path =~ s![\\/]$!!;
    my $thr = threads->new( \&_watcher, $path, $subtree, $queue);
    return { thread => $thr };
}

=head2 C<< ->watch_directory >>

  $w->watch_directory( path => $dir, subtree => 1 );

Add a directory to the list of watched directories.

=cut

sub watch_directory( $self, %options ) {
    my $dir = delete $options{ path };
    if( $self->watchers->{$dir}) {
        $self->unwatch_directory( path => $dir );
    }
    $self->watchers->{ $dir } = $self->build_watcher(
        queue => $self->queue,
        path => $dir,
        %options
    );
}

=head2 C<< ->unwatch_directory >>

  $w->unwatch_directory( path => $dir );

Remove a directory from the list of watched directories. There still may
come in some events stored for that directory previously in the queue.

=cut

sub unwatch_directory( $self, %options ) {
    my $dir = delete $options{ path };
    if( my $t = delete $self->watchers->{ $dir }) {
        my $thr = delete $t->{thread};
        $thr->kill('KILL');
        eval { $thr->join; }; # sometimes the thread is not yet joinable?!
    }
}

sub DESTROY($self) {
    if( my $w = $self->{watchers}) {
        for my $t (keys %$w) {
            $self->unwatch_directory( path => $t )
        }
    };
}

=head2 C<< ->wait $CB >>

  $w->wait(sub {
      my ($event) = @_;
      say $event->{action};
      say $event->{path};
  });

Synchronously wait for file system events.

=cut

sub wait( $self, $cb) {
    while( 1 ) {
        my @events = $self->queue->dequeue;
        for (@events) {
