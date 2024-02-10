package Filesys::DB::FTS::Thesaurus;
use 5.020;
use Moo 2;
use feature 'signatures';
no warnings 'experimental::signatures';

our $VERSION = '0.01';

use YAML 'LoadFile';

# HoA
has 'dictionary' => (
    is => 'lazy',
    default => sub { {} },
);

sub add( $self, $other ) {
    my $dictionary = $self->dictionary;

    while( my ($k, $v) = each(%$other)) {
        $v = [$v] unless ref $v;
        $dictionary->{ lc $k } //= [];
        push $dictionary->{ lc $k }->@*, @$v;
    }

    return $dictionary
}

sub load( $self, @filenames ) {
    $self = $self->new() if ! ref $self;

    for my $file (@filenames) {
        $self->add( LoadFile( $file ));
    }

    return $self
}

1;
