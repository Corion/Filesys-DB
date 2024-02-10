package URL::FilterSet;
use 5.020;
use Moo 2;

our $VERSION = '0.01';

use feature 'signatures';
no warnings 'experimental::signatures';

use Storable 'dclone';

=head1 NAME

URL::FilterSet - conveniently add/remove filters from a query

=cut

has 'filters' => (
    is => 'ro',
    default => sub { {} },
);

=head1 METHODS

=head2 C<< ->from_query >>

Creates a new filter set from a query parameter

=cut

sub from_query( $class, $query ) {
    my $self = $class->new();
    $query =~ s!^~!!;
    $query =~ s!~$!!;
    $self->add( split /[:~]/, $query );

use Data::Dumper;
warn Dumper $self->filters;


    return $self
}

sub has_filters( $self ) {
    keys $self->filters->%*
}

sub add( $self, @pairs ) {
    while( my( $c, $p) = splice @pairs, 0, 2 )  {
        $self->filters->{ $c } //= {};
        $self->filters->{ $c }->{ $p } = 1;
    }
}

sub remove( $self, @pairs ) {
    while( my( $c, $p) = splice @pairs, 0, 2 )  {
        delete $self->filters->{ $c }->{ $p };
        if( ! keys $self->filters->{ $c }->%* ) {
            delete $self->filters->{ $c };
        };
    }
}

=head2 C<< ->as_query >>

  $uri->query( { filter => $filters->as_query } )

Returns the filters as a canonicalized string.

=cut

sub as_query( $self, @pairs ) {
    my $s = $self->clone( @pairs );
    my $f = $s->filters;

    my $q = join "~",
        map {
            my $k = $_;
            map {
                "$k:$_"
            } sort keys $f->{ $k }->%*
        } sort keys $f->%*;
    return $q
}

sub clone( $self, @pairs ) {
    my $copy = $self;
    if( @pairs ) {
        $copy = dclone $self;
        $copy->add( @pairs );
    }
    return $copy;
}

=head2 C<< ->as_sql >>

Returns a list of pairs suitable for SQL::Abstract IN clauses.

=cut

sub as_sql( $self, @pairs ) {
    $self = $self->clone( @pairs );
    my $f = $self->filters;

    return map {
        my $k = $_;
        $k => [ sort keys $f->{ $k }->%* ],
    } sort keys $f->%*
}

1;
