package Filesys::TreeWalker;
use 5.020;
use feature 'signatures';
no warnings 'experimental::signatures';
use Exporter 'import';

use Carp 'croak';

our $VERSION = '0.01';
our @EXPORT_OK = ('scan_tree_bf');

=head1 NAME

Filesys::TreeWalker - walk a directory tree breadth first, newest entries first

=head1 SYNOPSIS

  use Filesys::TreeWalker 'scan_tree_bf';
  
  scan_tree_bf(
      file      => sub( $name, $context ) { say "$name: size is " . $context->{stat}->[7] },
      directory => sub( $name, $context ) { 1 },
  );

=head1 FUNCTIONS

=cut

sub _collect_fs_info( $fn, $parent=undef ) {
    $fn =~ s![/\\]\z!!; # strip off any directory separator as we'll use our own
    my $type = -f $fn ? 'file'
             : -d $fn ? 'directory'
             : undef;
    return {
        type   => $type,
        stat   => [stat($fn)],
        parent => $parent,
        name   => $fn,
    }
}

sub is_win32_reparse($fn) {
    if( $^O =~ /mswin32/i) {
        require Win32API::File;
        # require Win32::LongPath;
        my $fa = Win32API::File::GetFileAttributes($fn);
        return $fa & Win32API::File::FILE_ATTRIBUTE_REPARSE_POINT();
        #if( $fa & Win32::LongPath::FILE_ATTRIBUTE_REPARSE_POINT() ) {
        #    $fn = Win32::LongPath::readlinkL($fn)
        #        or die $^E;
        #}
    }
}

=head2 C<< scan_tree_bf %options >>

  scan_tree_bf(
      queue     => '/some/root/dir',
      file      => sub( $name, $context ) { say "$name: size is " . $context->{stat}->[7] },
      directory => sub( $name, $context ) { 1 },
      wanted    => sub( $dir, $context ) { $dir !~ /\.git$/i }
  );

Scans a directory tree breadth first, prioritizing the newest entries first.

=over 4

=item B<queue>

  queue => ['/path/to/dir1', '/opt/dir2'],

The root directory to be scanned. If you want multiple directories, pass an array reference.

=item B<file>

The callback that is invoked for each file. The callback is passed the full pathname and a context
hash that contains the L<stat> information and the parent directory name.

=item B<directory>

The callback that is invoked for each directory. The callback is passed the full pathname and a context
hash that contains the L<stat> information and the parent directory name.

The callback must return a true value if the directory should be scanned.

=item B<wanted>

A filtering subroutine that is invoked for each directory to determine whether
it should be scanned or not.

=back
  
=cut

# We currently expect entries from a filesystem, not ftp/webdav/ssh yet
sub scan_tree_bf( %options ) {
    my $on_file      = delete $options{ file } // sub {};
    my $on_directory = delete $options{ directory } // sub {};
    my $wanted       = delete $options{ wanted } // sub { 1 };
    my $queue        = delete $options{ queue } // ['.'];

    if( $queue and ! ref $queue) {
        $queue = [$queue];
    };

    for my $entry (@$queue) {
        if(! ref $entry ) {
            $entry = _collect_fs_info( $entry );
        }
    }

    while (@$queue) {
        my $entry = shift @$queue;

        if( $entry->{type} eq 'directory' ) {
            if( ! $on_directory->($entry->{name}, $entry)) {
                # we are actually not interested in this directory
                next;
            };

            my $dn = $entry->{name};

            # Resolve junctions on Windows, currently we skip those instead
            #if( is_win32_reparse($dn)) {
            #    next
            #};

            # $dn = win32_reparse($dn);
            # warn "[$dn]";
            opendir my $dh, $dn or croak "Couldn't read contents of '$dn': $!";
            my @entries = map {
                my $full = "$dn/$_";

                _collect_fs_info( $full, $dn )
            } grep {
                    $_ ne '.'
                and $_ ne '..'
                and !is_win32_reparse("$dn/$_")
                and $wanted->("$dn/$_")
            } readdir $dh;

            @$queue = sort { $b->{stat}->[9] <=> $a->{stat}->[9] } @$queue, @entries;

        } elsif( $entry->{type} eq 'file' ) {
            $on_file->($entry->{name}, $entry);

        } else {
            # we skip stuff that is neither a file nor a directory
        }
    }
};

1;

=head1 SEE ALSO

L<File::Find> - a depth-first walker

=cut