package Filesys::Notify::Win32::ReadDirectoryChanges;
use 5.020;

use Moo 2;
use feature 'signatures';
no warnings 'experimental::signatures';

# First prototype
use File::Spec;
use Win32::API;
use Win32API::File 'CreateFile', 'CloseHandle', ':FILE_FLAG_', 'FILE_LIST_DIRECTORY', 'OPEN_EXISTING', 'FILE_SHARE_WRITE', 'FILE_SHARE_READ', 'GENERIC_READ';
use threads; # we launch a thread for each watched tree to keep the logic simple
use Thread::Queue;
use Encode 'decode';

our $VERSION = '0.01';

=head1 NAME

Filesys::Notify::Win32::ReadDirectoryChanges - read directory changes

=SYNOPSIS

  my $watcher = Filesys::Notify::Win32::ReadDirectoryChanges->new();
  for my $dir (@ARGV) {
      $watcher->watch_directory( path => $dir, subtree => 1 );
  };
  $watcher->watch(sub {
      my( $event ) = @_;
      say $event->{action}, ":", $event->{path};
  });

This module spawns a thread for each watched directory. Each such thread synchronously
reads file system changes and communicates them to the main thread through a L<Thread::Queue>.

=head1 METHODS

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

Win32::API->Import( 'kernel32.dll', 'ReadDirectoryChangesW', 'NPNNNPNN','N' )
    or die $^E;
Win32::API->Import( 'kernel32.dll', 'CancelIoEx', 'NN','N' )
    or die $^E;

sub _unpack_file_notify_information( $buf ) {
# typedef struct _FILE_NOTIFY_INFORMATION {
#   DWORD NextEntryOffset;
#   DWORD Action;
#   DWORD FileNameLength;
#   WCHAR FileName[1];
# } FILE_NOTIFY_INFORMATION, *PFILE_NOTIFY_INFORMATION;

    state @action = (
        'unknown',
        'added',
        'removed',
        'modified',
        'old_name',
        'new_name',
    );

    my @res;
    my $ofs = 0;
    do {
        my ($next, $action, $fn ) = unpack 'VVV/a', $buf;
        $ofs = $next;
        $fn = decode( 'UTF-16le', $fn );
        push @res, { action => $action[ $action ], path => $fn };
        $buf = substr($buf, $next);
    } while $ofs > 0;
    @res
}

sub _ReadDirectoryChangesW( $hDirectory, $watchSubTree, $filter ) {
    my $buffer = "\0" x 65520;
    my $returnBufferSize = "\0" x 4;
    my $r = ReadDirectoryChangesW(
        $hDirectory,
        $buffer,
        length($buffer),
        !!$watchSubTree,
        $filter,
        $returnBufferSize,
        0,
        0);
    if( $r ) {
        $returnBufferSize = unpack 'V', $returnBufferSize;
        return substr $buffer, 0, $returnBufferSize;
    } else {
        return undef
    }
}

# Add ReadDirectoryChangesExW support
# Consider sub backfillExtendedInformation($fn,$info) {
# }

sub build_watcher( $self, %options ) {
    my $path = delete $options{ path };
    my $subtree = !!( $options{ subtree } // $self->subtree );
    my $queue = $self->queue;
    # XXX check if/how we can pass in UTF-8 names, and if we need to encode them to UTF-16LE first
    my $hPath = CreateFile( $path, FILE_LIST_DIRECTORY()|GENERIC_READ(), FILE_SHARE_READ() | FILE_SHARE_WRITE(), [], OPEN_EXISTING(), FILE_FLAG_BACKUP_SEMANTICS(), [] )
        or die $^E;
    $path =~ s![\\/]$!!;
    my $thr = threads->new( sub($path,$hPath,$subtree,$queue) {
        my $running = 1;
        while($running) {
            # 0x1b means 'DIR_NAME|FILE_NAME|LAST_WRITE|SIZE' = 2|1|0x10|8
            my $res = _ReadDirectoryChangesW($hPath, $subtree, 0x1b);

            if( ! defined $res ) {
                if( $^E != 995 ) { # ReadDirectoryChangesW got cancelled and we should quit
                    warn $^E;
                }
                last
            }

            for my $i (_unpack_file_notify_information($res)) {
                $i->{path} = File::Spec->catfile( $path , $i->{path} );
                $queue->enqueue($i);
            };
        }
    }, $path, $hPath, $subtree, $queue);
    return { thread => $thr, handle => $hPath };
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
        CancelIoEx($t->{handle},0);
        CloseHandle($t->{handle});
        my $thr = delete $t->{thread};
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
    while( my @events = $self->queue->dequeue) {
        $cb->($_) for @events;
    };
}

1;

=head1 SEE ALSO

L<Filesys::Notify::Simple>

L<Filesys::Notify>

L<Win32::ChangeNotify>

Currently, no additional information like that available through L<https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-readdirectorychangesexw|ReadDirectoryChangesExW>
is collected. But a wrapper/emulation could provide that information whenever RDCE is unavailable (on Windows versions before Windows 10).

=cut