package Filesys::Notify::Win32::ReadDirectoryChanges;
use 5.020;

use Moo 2;
use feature 'signatures';
no warnings 'experimental::signatures';

# First prototype
#use Win32::IPC;
#use Win32::ChangeNotify;
use Win32::API;
use Win32API::File 'CreateFile', ':FILE_FLAG_', 'FILE_LIST_DIRECTORY', 'OPEN_EXISTING', 'FILE_SHARE_WRITE', 'FILE_SHARE_READ', 'GENERIC_READ';
use threads; # we launch a thread for each watched tree to keep the logic simple
use Thread::Queue;
use Encode 'decode';

has 'watchers' => (
    is => 'lazy',
    default => sub{ +{} },
);

has 'queue' => (
    is => 'lazy',
    default => sub { Thread::Queue->new() },
);

Win32::API->Import( 'kernel32.dll', 'ReadDirectoryChangesW', 'NPNNNPNN','N' )
        or die $^E;

sub _unpack_file_notify_information( $buf ) {
# typedef struct _FILE_NOTIFY_INFORMATION {
#   DWORD NextEntryOffset;
#   DWORD Action;
#   DWORD FileNameLength;
#   WCHAR FileName[1];
# } FILE_NOTIFY_INFORMATION, *PFILE_NOTIFY_INFORMATION;
    my @res;
    my $ofs = 0;
    do {
        my ($next, $action, $fn ) = unpack 'VVV/a', $buf;
        $ofs = $next;
        $fn = decode( 'UTF-16le', $fn );
        push @res, { action => $action, path => $fn };
        $buf = substr($buf, $next);
    } while $ofs > 0;
    @res
}

sub _ReadDirectoryChangesW( $hDirectory, $watchSubTree, $filter ) {
    my $buffer = "\0" x 65520;
    my $returnBufferSize = "\0" x 4;
    ReadDirectoryChangesW(
        $hDirectory,
        $buffer, 
        length($buffer),
        1,
        $filter,
        $returnBufferSize,
        0,
        0)
        or die $^E;
    $returnBufferSize = unpack 'V', $returnBufferSize;
    return substr $buffer, 0, $returnBufferSize;
}

sub build_watcher( $self, $queue, $path ) {
    my $hPath = CreateFile( $path, FILE_LIST_DIRECTORY()|GENERIC_READ(), FILE_SHARE_READ() | FILE_SHARE_WRITE(), [], OPEN_EXISTING(), FILE_FLAG_BACKUP_SEMANTICS(), [] )
        or die $^E;
    my $thr = async {
        while(1) {
            # 0x1b means 'DIR_NAME|FILE_NAME|LAST_WRITE|SIZE' = 2|1|0x10|8
            my $res = _ReadDirectoryChangesW($hPath, 1, 0x1b);
            for my $i (_unpack_file_notify_information($res)) {
                $queue->enqueue($i);
            };
        }
    };
    return $thr;
}

sub watch_directory( $self, $dir ) {
    $self->watchers->{ $dir } = $self->build_watcher( $self->queue, $dir );
}

sub unwatch_directory( $self, $dir ) {
    if( my $t = delete $self->watchers->{ $dir }) {
        $t->kill;
    }
}

sub DESTROY($self) {
    if( my $w = $self->{watchers}) {
        for my $t (values %$w) {
            $t->kill
        }
    };
}

sub wait( $self, $cb) {
    while( my @events = $self->queue->dequeue) {
        $cb->($_) for @events;
    };
}

1;