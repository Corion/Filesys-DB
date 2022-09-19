#perl
use strict;
use 5.010;
use Test::More tests => 4;

use Filesys::Notify::Win32::ReadDirectoryChanges;
use File::Temp 'tempfile';
use File::Basename;
use threads;

my ($fh1,$tempname1) = tempfile( UNLINK => 1 );
my $tempdir = dirname($tempname1);
close $fh1;

# spirit the thread away in a subroutine so we
# don't close over the file watcher
sub do_stuff {
    my ($fh2,$tempname2) = tempfile(UNLINK => 0);
    close $fh2;

    my $t = async {
        note "Temp name 2: $tempname2";
        open $fh2, '>', $tempname2;

        print {$fh2} "Hello World\n";
        close $fh2;
        unlink $tempname2 or warn $!;
    };
    return ($tempname2,$t);
}

my $w = Filesys::Notify::Win32::ReadDirectoryChanges->new(
    directory => [$tempdir]
);
sleep 1;

note "Temp dir: $tempdir";
my ($tempname2,$t) = do_stuff();
sleep 1;

$w->unwatch_directory( path => $tempdir );

my @actions2 = ('added','modified','modified','removed');

while ( my $ev = $w->queue->dequeue ) {
    note "$ev->{path}: $ev->{action}";
    if( $ev->{path} eq $tempname2 ) {
        state $idx = 0;
        my $act = $actions2[$idx++];
        is $ev->{action}, $act, "second tempfile: $act";
        last if $idx == @actions2;
    };
}
note "Cleanup";
$t->join;
note "GLOBAL";