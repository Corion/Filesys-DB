package Filesys::DB;
use 5.020;
use Moo 2;
use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';

use DBI ':sql_types';
use DBD::SQLite;

use PadWalker 'var_name'; # for scope magic...
use DBIx::RunSQL;
use Encode 'encode', 'decode', '_utf8_off';
use JSON 'encode_json', 'decode_json';

use Carp 'croak';

use lib '../Weather-MOSMIX/lib';
with 'MooX::Role::DBIConnection';

has 'mountpoints' => (
    is => 'ro',
    default => sub { {} },
);

around BUILDARGS => sub ($orig, $class, @args) {
    my $args = @args == 1 && ref $args[0] ? $args[0] : { @args };
    $args->{ dbh } //= {};
    $args->{ dbh }->{dsn} //= 'dbi:SQLite:dbname=db/filesys-db.sqlite';
    $args->{ dbh }->{options} //= { RaiseError => 1, PrintError => 0 };
    return $class->$orig($args)
};

# This should go into a separate DBIx role, likely
sub selectall_named {
    # No subroutine signature since we need to preserve the aliases in @_
    my( $self, $sql ) = splice @_, 0, 2;

    # Gather the names of the variables used in the routine calling us
    my %parameters = map {
        var_name(1, \$_) => $_
    } @_;

    my $sth;
    if( ref $sql ) {
        $sth = $sql;
    } else {
        my $dbh = $self->dbh;
        $sth = $dbh->prepare($sql);
    };
    my $parameter_names = $sth->{ParamValues};

    while (my ($name,$value) = each %$parameter_names) {
        (my $perl_name) = ($name =~ m!(\w+)!);
        $perl_name = '$' . $perl_name;
        if( ! exists $parameters{$perl_name}) {
            croak "Missing bind parameter '$perl_name'";
        };
        #use Data::Dumper; local $Data::Dumper::Useqq = 1;
        #warn "$name => " . Dumper($parameters{$perl_name});
        $sth->bind_param($name => $parameters{$perl_name}, SQL_VARCHAR)
    };

    $sth->execute;
    # we also want to lock the hashes we return here, I guess
    return $sth->fetchall_arrayref({})
};

sub get_mountpoint_alias( $self, $filename ) {
    my $longest;
    my $mp = $self->mountpoints;
    for my $alias (sort { length $mp->{$b} <=> length $mp->{$a} || $a cmp $b } keys %$mp) {
        if( index( $filename, $mp->{$alias} ) == 0 ) {
            return ($mp->{$alias}, $alias)
        }
    }
    croak "Don't know how/where to store '$filename', no mountpoint found.";
}

sub to_alias( $self, $filename ) {
    my ($mp,$alias) = $self->get_mountpoint_alias( $filename );
    $filename =~ s!^\Q$mp\E!$alias!;
    return $filename
}

sub to_local( $self, $filename ) {
    my $mp = $self->mountpoints;
    for my $alias (reverse sort { length $a <=> length $b || $a cmp $b } keys %{ $mp }) {
        if( index( $filename, $alias ) == 0 ) {
            $filename =~ s!^\Q$alias\E!$mp->{$alias}!;
            return $filename
        }
    }
    croak "Unknown mountpoint in '$filename'.";
}

# here, we take the path as primary key:
sub insert_or_update_direntry( $self, $info ) {
    my $local_filename = $info->{filename};
    $info->{filename} = $self->to_alias( $info->{filename});
    my $value = encode_json( $info );

    # Clean out all values that should not be stored:
    delete @{$info}{ (grep { /^_temp/ } keys %$info) };

    my $res;
    if( defined $info->{entry_id}) {
        my $entry_id = $info->{entry_id};
        my $tmp_res = $self->selectall_named(<<'SQL', $value, $entry_id )->[0];
            insert into filesystem_entry (entry_id, entry_json)
            values (:entry_id, :value)
            on conflict(entry_id) do
            update set entry_json = :value
            returning entry_id
SQL
        $res = $tmp_res->{entry_id};

    } else {
        # $info->{filename} must be unique
        my $tmp_res = $self->selectall_named(<<'SQL', $value );
            insert into filesystem_entry (entry_json)
            values (:value)
            on conflict(filename) do
            update set entry_json = :value
            returning entry_id
SQL
        $res = $tmp_res->[0]->{entry_id};
    };
    $info->{entry_id} = $res;
    $info->{filename} = $local_filename;
    return $info
}

# here, we take the path as primary key:
sub find_direntry_by_filename( $self, $filename ) {
    $filename = $self->to_alias($filename);

    # All filenames will be UTF-8 encoded, as they live in a JSON blob,
    # no matter their original encoding:
    $filename = encode('UTF-8', $filename );

    my $entry = $self->selectall_named(<<'SQL', $filename);
        select entry_json
             , entry_id
          from filesystem_entry
        where filename = :filename
SQL
    my $res;
    if( @$entry ) {
        $res = decode_json( $entry->[0]->{entry_json} );
        # The filename comes back as an UTF-8 string, but we want to
        # get at the original octets that we originally stored:
        $res->{filename} = decode( 'UTF-8', $res->{filename}); # really?!
        _utf8_off($res->{filename}); # a filename is octets, not UTF-8
        $res->{filename} = $self->to_local($res->{mountpoint}, $res->{filename});
        $res->{entry_id} = $entry->[0]->{entry_id};
    };
    return $res
}

1;
