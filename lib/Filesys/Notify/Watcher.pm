package Filesys::Notify::Watcher;
use 5.020;
use experimental 'signatures';

our $VERSION = '0.01';

use File::ChangeNotify;
use Moo 2;
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
        $self->watch_directory( path => $dirs );
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

has 'thread' => (
    is => 'rw',
);

sub _watcher($paths,$subtree,$queue) {
    my $running = 1;
    my $w = File::ChangeNotify->instantiate_watcher(directories => $paths);
    state %translate = (
        create => 'added',
        modify => 'modified',
        delete => 'removed',
        unknown => 'unknown',
    );
    local $SIG{KILL}=sub{$running=0; threads->exit};
    while($running) {
        my @events = $w->wait_for_events;

        for my $ev (@events) {
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

sub stop_watcher($self) {
    if( my $t = $self->thread) {
        my $thr = $t->{thread};
        eval { $thr->kill('KILL'); $thr->detach; }; # sometimes the thread is not yet joinable?!
        $self->thread(undef);
    }
}

sub restart_watcher($self, %options) {
    $self->stop_watcher();
    my $d = [keys %{$self->watchers}];
    if(@$d) {
    $self->thread( $self->build_watcher(
        queue => $self->queue,
        path => [keys %{$self->watchers}],
        %options
    ));
    }
}

=head2 C<< ->watch_directory >>

  $w->watch_directory( path => $dir, subtree => 1 );

Add a directory to the list of watched directories.

=cut

sub watch_directory( $self, %options ) {
    my $dir = delete $options{ path };
    $dir = [$dir] if !ref $dir;
    $self->watchers->{ $_ } = 1 for @$dir;
    $self->restart_watcher();
}

=head2 C<< ->unwatch_directory >>

  $w->unwatch_directory( path => $dir );

Remove a directory from the list of watched directories. There still may
come in some events stored for that directory previously in the queue.

=cut

sub unwatch_directory( $self, %options ) {
    my $dir = delete $options{ path };
    $dir = [$dir] if !ref $dir;
    delete $self->watchers->{ $_ } for @$dir;
    $self->restart_watcher();
}

sub DESTROY($self) {
    $self->stop_watcher();
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
            if( defined $_ ) {
                $cb->($_);
            } else {
                # somebody did ->queue->enqueue(undef) to stop us
                last;
            }
        };
    };
}

1;
