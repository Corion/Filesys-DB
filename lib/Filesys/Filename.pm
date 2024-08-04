package Filesys::Filename;
use 5.020;
use Moo;
use Encode 'encode';
use Encode 'decode';

use feature 'signatures';
no warnings 'experimental::signatures';

our $VERSION = '0.01';

=head1 NAME

Filesys::Filename - store the filesystem encoding with the filename

=cut

# XXX this guess should be refined
our $default_encoding = $^O eq 'MSWin32' ? 'Latin-1' : 'UTF-8';

has 'encoding' => (
    is => 'ro',
    default => sub { $default_encoding },
);

has 'value' => (
    is => 'ro',
    required => 1,
);

sub from_native( $self, $str, $encoding = $default_encoding ) {
    $str =~ s!\\!/!g;
    $self->new( encoding => $encoding, value => decode( $default_encoding => $str ))
}

sub native( $self ) {
    encode( $self->encoding, $self->value )
}

sub TO_JSON( $self ) {
    $self->value
}

# Delegate stuff to Path::Class? Or some other helper?

1;
