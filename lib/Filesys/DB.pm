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

our $VERSION = '0.01';

=head1 NAME

Filesys::DB - store and access filesystem metadata in SQLite

=cut

# All mountpoints need to end in "/" or "\\" , except that we don't enforce that yet
has 'mountpoints' => (
    is => 'ro',
    default => sub { {} },
);

around BUILDARGS => sub ($orig, $class, @args) {
    my $args = @args == 1 && ref $args[0] ? $args[0] : { @args };
    $args->{ dbh } //= {};
    $args->{ dbh }->{dsn} //= 'dbi:SQLite:dbname=db/filesys-db.sqlite';
    $args->{ dbh }->{options} //= {
        RaiseError => 1,
        PrintError => 0,
        # No - we want raw bytes in and out, as we'll do the en/decoding ourselves
        #sqlite_unicode => 1, #$use_unicode
    };

    if( $args->{mountpoints}) {
        __PACKAGE__->_restructure_mountpoints( $args->{mountpoints});
    }
    return $class->$orig($args)
};

sub _restructure_mountpoints( $self, $mountpoints ) {
    for my $mp (keys %{ $mountpoints}) {
        if( ! ref $mountpoints->{$mp} ) {
            $mountpoints->{$mp} = +{ directory => $mountpoints->{$mp} };
        };
        # Backfill the alias into the structure
        $mountpoints->{$mp}->{alias} = $mp;
    }
}

=head2 C<< ->init_config >>

  $store->init_config(
      default_config => 'filesys-db.yaml',
      filename       => $filename_from_ARGV,
  );

Looks for a config file and a default config file, and initialize from there

=cut

sub init_config( $self, %options ) {
    my $config = {};
    my $user_config = {};
    $options{ config_file } //= $options{ default_config_file };
    if(! defined $options{ config_file } ) {
        $user_config = {
            mountpoints => [
                {
                    alias => $options{ mount_alias } // '${MOUNT}',
                    directory => $options{ mountpoint } //  $ARGV[0],
                }
            ],
        }
    }
    if( -f $options{ config_file }) {
        $user_config = LoadFile( $options{ config_file });
    };
    $user_config->{mountpoints} //= {};

    # Should we merge or simply replace?!

    $self->{mountpoints} = $user_config->{mountpoints};
    if( $options{mountpoints}) {
        $self->_restructure_mountpoints( $self->{mountpoints});
    }
    return ($config, $user_config);
}

# This should go into a separate DBIx role, likely
sub bind_lexicals( $self, $sql, $level, $lexicals ) {
    my $sth;
    if( ref $sql ) {
        $sth = $sql;
    } else {
        my $dbh = $self->dbh;
        $sth = $dbh->prepare($sql);
    };
    return $sth unless $lexicals;

    # Gather the names of the variables used in the routine calling us
    my %parameters = map {
        if( ! var_name($level, \$_)) {
            my $real_level = 1;
            my $name;
            do { eval {
                $name = var_name($real_level++, \$_);
            }} until defined $name or $@ or $real_level > $level+10;
            croak "Mapping variable at level $level containing <$_>, but found at $real_level";
        };
        var_name($level, \$_) => $_
    } @$lexicals;

    my $parameter_names = $sth->{ParamValues};

    while (my ($name,$value) = each %$parameter_names) {
        (my $perl_name) = ($name =~ m!(\w+)!);
        $perl_name = '$' . $perl_name;
        if( ! exists $parameters{$perl_name}) {
            croak "Missing bind parameter '$perl_name'";
        };
        my $type = SQL_VARCHAR;

        # This is a horrible API, but so is using uplevel'ed variables
        if( my $r = ref $parameters{$perl_name}) {
            if( $r eq 'SCALAR' ) {
                $type = SQL_INTEGER;
                # Clear out old variable binding:
                my $v = $parameters{$perl_name};
                delete $parameters{$perl_name};
                $parameters{$perl_name} = $$v;
            } elsif( $r eq 'ARRAY' ) {
                $type = SQL_INTEGER;
                # Clear out old variable binding:
                my $v = $parameters{$perl_name};
                delete $parameters{$perl_name};
                $parameters{$perl_name} = $v->[0];
                $type = $v->[1];
            }
        }
        $sth->bind_param($name => $parameters{$perl_name}, $type)
    };

    return $sth
}

sub execute_named_ex( $self, %options ) {
    $options{ level } //= 1;
    my $sql = $options{ sth } // $options{ sql };
    my $sth = $self->bind_lexicals( $sql, $options{ level }+1, $options{ lexicals });
    $sth->execute;
    # we also want to lock the hashes we return here, I guess
    return $sth
};

sub execute_named {
    my( $self, $sql ) = splice @_, 0, 2;
    return $self->execute_named_ex(
        sth => $sql,
        lexicals => [@_],
        level => 2,
    );
};

sub selectall_named_ex($self, %options) {
    $options{ level } //= 1;
    my $sth = $self->bind_lexicals( $options{ sth }, $options{ level }+1, $options{ lexicals });
    $sth->execute;
    # we also want to lock the hashes we return here, I guess
    return $sth->fetchall_arrayref({})
}

sub selectall_named {
    my( $self, $sql ) = splice @_, 0, 2;

    my $lex = \@_;
    my $sth = $self->selectall_named_ex(
        sth => $sql,
        level => 2,
        lexicals => $lex,
    );
}

sub get_mountpoint_alias( $self, $filename ) {
    my $longest;
    my $mp = $self->mountpoints;
    for my $alias (sort {    length $mp->{$b}->{directory} <=> length $mp->{$a}->{directory}
                          || $a cmp $b # do we really want to compare the names here?!
                        } keys %$mp) {
        if( index( $filename, $mp->{$alias}->{directory} ) == 0 ) {
            return ($mp->{$alias}->{directory}, $alias)
        }
    };
    croak "Don't know how/where to store '$filename', no mountpoint found for that directory.";
}

sub to_alias( $self, $filename ) {
    my ($mp,$alias) = $self->get_mountpoint_alias( $filename );
    #$filename =~ s!^\Q$mp\E[\\/]!!
    #    or die "Could't strip mountpoint prefix '$mp' from $filename";
    substr($filename, 0, length($mp)+1) = '';
    return ($alias,$filename)
}

sub to_local( $self, $mountpoint, $filename ) {
    my $mp = $self->mountpoints;
    if( ! exists $self->mountpoints->{ $mountpoint }) {
        croak "Unknown mountpoint '$mountpoint'";
    }
    # XXX make this FS dependent, don't blindly use '/'
    # XXX Also, don't assume that mountpoints are in UTF-8 (but that's what YAML gives us)
    return encode('UTF-8', $self->mountpoints->{ $mountpoint }->{directory}) . '/' . $filename;
}

# here, we take the path as primary key:
sub insert_or_update_direntry( $self, $info ) {
    my $local_filename = $info->{filename};
    (my($mountpoint), $info->{filename}) = $self->to_alias( $info->{filename});

    $info->{mountpoint} //= $mountpoint;

    # Sanity check - we don't want to store anything looking like an
    # absolute filename
    if( $local_filename eq $info->{filename}) {
        if( $mountpoint ) {
            croak "Can't store absolute filename '$local_filename', didn't remove the mountpoint '$mountpoint' " . $self->mountpoints->{$mountpoint}->{directory};
        } else {
            croak "Can't store absolute filename '$local_filename', didn't find/resolve the mountpoint";
        }
    }
    if( $info->{filename} =~ m!^/!) {
        croak "Can't store absolute filename '$info->{filename}'";
    }

    # If we clean out, we need to do so on a copy!
    # Clean out all values that should not be stored:
    # delete @{$info}{ (grep { /^_temp/ } keys %$info) };

    my $value = encode_json( $info );

    my $res;
    if( defined $info->{entry_id}) {
        my $entry_id = $info->{entry_id};
        my $tmp_res = $self->selectall_named(<<'SQL', $value, $entry_id )->[0];
            insert into filesystem_entry (entry_id, entry_json)
            values (:entry_id, :value)
            on conflict(entry_id) do
            update set entry_json = :value
            returning entry_id, filename
SQL
        $res = $tmp_res->{entry_id};

    } else {
        # $info->{filename} must be unique
        my $tmp_res = $self->selectall_named(<<'SQL', $value );
            insert into filesystem_entry (entry_json)
            values (:value)
            on conflict(mountpoint,filename) do
            update set entry_json = :value
            returning entry_id, filename
SQL
        $res = $tmp_res->[0]->{entry_id};
    };
    $info->{entry_id} = $res;
    $info->{filename} = $local_filename;
    return $info
}

sub _inflate_entry( $self, $entry ) {
    # Downgrade the string again:
    my $res = decode_json( $entry->{entry_json} );

    # The filename comes back as an UTF-8 string, but we want to
    # get at the original octets that we originally stored:
    _utf8_off($res->{filename}); # a filename is octets, not UTF-8
    $res->{filename} = decode( 'UTF-8', $res->{filename}); # really?!
    $res->{filename} = encode( 'Latin-1', $res->{filename}); # really?! Convert back to "octets"
    _utf8_off($res->{filename}); # a filename is octets, not UTF-8
    $res->{filename} = $self->to_local($res->{mountpoint}, $res->{filename});
    $res->{entry_id} = $entry->{entry_id};
    return $res
}

sub _inflate_sth( $self, $sth ) {
    return map {
        $self->_inflate_entry( $_ );
    } @{ $sth->fetchall_arrayref( {} )}
}

# here, we take the path as primary key:
sub find_direntry_by_filename( $self, $filename ) {
    (my($mountpoint), $filename) = $self->to_alias($filename);

    # All filenames will be UTF-8 encoded, as they live in a JSON blob,
    # no matter their original encoding:
    $filename = encode('UTF-8', $filename );

    my $res = $self->selectall_named(<<'SQL', $filename, $mountpoint);
        select entry_json
             , entry_id
          from filesystem_entry
        where filename = :filename
          and mountpoint = :mountpoint
SQL

    if( @$res ) {
        $res->[0] = $self->_inflate_entry( $res->[0] );
    }
    return $res->[0]
}

=head2 C<< ->integrity_check >>

  my $problems = $fs->integrity_check()->fetchall_arrayref({});
  for my $entry (@$problems) {
      say "$entry->{filename} ($entry->{entry_id}): $entry->{reason}";
  }

Run some integrity checks on the database.

=cut

sub integrity_check( $self ) {
    my @res;
    my $invalid_mountpoint = $self->execute_named( <<'SQL' );
        select entry_id
             , filename
             , 'Empty mountpoint, run fix-mountpoint' as reason
          from filesystem_entry
        where mountpoint is null
           or mountpoint = ''
SQL
    return $invalid_mountpoint
}

# No prototype since we want to capture the variables passed in:
sub entries_ex( $self, %options ) {
    $options{ level } //= 0;
    $options{ level } += 2;
    $options{ columns } //= [qw[entry_id entry_json]];

    my $columns = join ",", map {qq("$_")} @{ $options{ columns }};

    my $where = delete $options{ where }
        or croak "No where clause";
    my $entries = $self->execute_named_ex(sql => <<"SQL", %options);
         select $columns
           from filesystem_entry
          where 1=1
            and $where
SQL
}
# No prototype since we want to capture the variables passed in:
sub entries {
    my( $self, $columns, $where ) = splice @_,0,3;
    return $self->entries_ex(columns => $columns, where => $where, lexicals => \@_, level => 2);
}

1;
