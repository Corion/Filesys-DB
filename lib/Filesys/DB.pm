package Filesys::DB;
use 5.020;
use Moo 2;
#use Filter::signatures;
use experimental 'signatures';

use DBI ':sql_types';
use DBD::SQLite;

use PadWalker 'var_name'; # for scope magic...
use DBIx::RunSQL;
use Encode 'encode', 'decode', '_utf8_off';
use JSON;
use YAML 'LoadFile';

use Carp 'croak';

with 'MooX::Role::DBIConnection';
use Filesys::Filename;

our $VERSION = '0.01';

=head1 NAME

Filesys::DB - store and access filesystem metadata in SQLite

=cut

# All mountpoints need to end in "/" or "\\" , except that we don't enforce that yet
has 'mountpoints' => (
    is => 'ro',
    default => sub { {} },
);

has 'json' => (
    is => 'ro',
    default => sub { JSON->new->convert_blessed },
);

# XXX this should be better/smarter, some day
our $default_encoding = $^O eq 'MSWin32' ? 'Latin-1' : 'UTF-8';

around BUILDARGS => sub ($orig, $class, @args) {
    my $args = @args == 1 && ref $args[0] ? $args[0] : { @args };
    # This should potentially also come from the config?!
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
            $mountpoints->{$mp} = +{
                directory => Filesys::Filename->from_native( $mountpoints->{$mp} ),
                encoding  => $default_encoding,
            };
        };
        # Backfill the alias into the structure
        if( ! ref $mountpoints->{$mp}->{directory} ) {
            $mountpoints->{$mp}->{directory} = Filesys::Filename->from_native(
                $mountpoints->{$mp}->{directory}, $mountpoints->{$mp}->{encoding},
            );
        }
        $mountpoints->{$mp}->{alias} //= $mp;
        $mountpoints->{$mp}->{encoding} //= $default_encoding;
        # XXX create the filesystem encoding, or guess it from somewhere
    }
    return
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
        my $alias = $options{ mount_alias } // '${MOUNT}';
        $user_config = {
            mountpoints =>
                {  $alias => {
                      alias => $alias,
                      directory => Filesys::Filename->from_native( $options{ mountpoint } //  $ARGV[0] ),
                      encoding => $default_encoding,
                   },
                }
        }
    } elsif ( -f $options{ config_file }) {
        $user_config = LoadFile( $options{ config_file });
    };
    $user_config->{mountpoints} //= {};

    # Should we merge or simply replace?!

    $self->{mountpoints} = $user_config->{mountpoints};
    if( keys %{$self->{mountpoints}}) {
        $self->_restructure_mountpoints( $self->{mountpoints});
    }
    return ($config, $user_config);
}

# This should go into a separate DBIx role, likely
# We could Memoize/cache this with the caller location
sub bind_lexicals( $self, $sql, $level, $lexicals ) {
    croak "Need an SQL string or a prepared DB handle"
        unless $sql;

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

    return $self->bind_named( $sql, \%parameters );
}

sub bind_named( $self, $sql, $parameters ) {
    my $sth;
    if( ref $sql ) {
        $sth = $sql;
    } else {
        my $dbh = $self->dbh;
        my $ok = eval {
            $sth = $dbh->prepare_cached($sql);
            #warn $sql;
            1;
        };
        if( ! $ok ) {
            croak "$@\nOffending SQL: $sql";
        };
    };

    my $parameter_names = $sth->{ParamValues};

    while (my ($name,$value) = each %$parameter_names) {
        (my $perl_name) = ($name =~ m!(\w+)!);
        $perl_name = '$' . $perl_name;
        if( ! exists $parameters->{$perl_name}) {
            croak "Missing bind parameter '$perl_name'";
        };
        my $type = SQL_VARCHAR;

        # This is a horrible API, but so is using uplevel'ed variables
        if( my $r = ref $parameters->{$perl_name}) {
            if( $r eq 'SCALAR' ) {
                $type = SQL_INTEGER;
                # Clear out old variable binding:
                my $v = $parameters->{$perl_name};
                delete $parameters->{$perl_name};
                $parameters->{$perl_name} = $$v;
            } elsif( $r eq 'ARRAY' ) {
                $type = SQL_INTEGER;
                # Clear out old variable binding:
                my $v = $parameters->{$perl_name};
                delete $parameters->{$perl_name};
                $parameters->{$perl_name} = $v->[0];
                $type = $v->[1];
            }
        }
        $sth->bind_param($name => $parameters->{$perl_name}, $type)
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
    # Shouldn't this just be ->execute_named_ex ?!
    my $sql = $options{ sth } // $options{ sql };
    my $sth = $self->bind_lexicals( $sql, $options{ level }+1, $options{ lexicals });
    if(! eval {
        $sth->execute;
        1;
    }) {
        croak "$@\n$sql";
    }
    # we also want to lock the hashes we return here, I guess
    return $sth->fetchall_arrayref({})
}

sub selectall_named {
    my( $self, $sql ) = splice @_, 0, 2;

    my $lex = \@_;
    return $self->selectall_named_ex(
        sql   => $sql,
        level => 2,
        lexicals => $lex,
    );
}

sub get_mountpoint_alias( $self, $_filename ) {
    my $mp = $self->mountpoints;
    for my $alias (sort {    length $mp->{$b}->{directory}->value <=> length $mp->{$a}->{directory}->value
                          || $a cmp $b # do we really want to compare the names here?!
                        } keys %$mp) {
        my $filename;
        if( ! ref $_filename ) {
            $filename = Filesys::Filename->from_native( $_filename, $mp->{$alias}->{encoding} );
        } else {
            $filename = $_filename;
        }
        my $fn = $filename->value;

        if( index( $fn, $mp->{$alias}->{directory}->value ) == 0 ) {
            return ($mp->{$alias}->{directory}, $alias)
        }
    };
    my $vis
        = ref $_filename ? $_filename->value : $_filename;
    croak sprintf "Don't know how/where to store '$vis', no mountpoint found for that directory.";
}

=head2 C<< ->decode_filename $filename_octets >>

  my $filename = $store->decode_filename( $octets );

Decodes a filename in the file-system local encoding to Unicode.

=cut

sub decode_filename( $self, $filename, $mountpoint = undef ) {
    if( ! ref $filename ) {
        my ($mp,$alias);
        if( ! $mountpoint ) {
            ($mp,$alias) = $self->get_mountpoint_alias( $filename );
        } else {
            $alias = $mountpoint;
        }
        $filename = Filesys::Filename->from_native(
            $filename,
            $self->mountpoints->{$alias}->{encoding}
        );
    }
    return $filename
}

sub to_alias( $self, $filename ) {
    my ($mp,$alias) = $self->get_mountpoint_alias( $filename );

    $filename = $self->decode_filename( $filename );

    substr($filename->{value}, 0, length($mp->value)+1) = '';

    return ($alias,$filename)
}

=head2 C<< ->to_local >>

  my $f = $store->to_local( 'documents', $filename );
  say sprintf "%s is %d bytes", $filename, -s $f;

Return a local filename, as octets. You can perform
file operations on the result string.

=cut

sub to_local( $self, $mountpoint, $filename ) {
    my $mp = $self->mountpoints->{ $mountpoint };
    if( !$mp ) {
        croak "Unknown mountpoint '$mountpoint'";
    }

    $filename = $self->decode_filename( $filename, $mountpoint );

    return $mp->{directory}->native . '/' . $filename->native;
}

# here, we take the path or entry_id as primary key:
sub insert_or_update_direntry( $self, $info ) {
    my $local_filename = $info->{filename};

    (my($mountpoint), $info->{filename}) = $self->to_alias( $info->{filename});

    $info->{mountpoint} //= $mountpoint;

    # Sanity check - we don't want to store anything looking like an
    # absolute filename
    if( $local_filename eq $info->{filename}->value) {
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

    my $value = $self->json->encode( $info );

    my $res;
    if( defined $info->{entry_id}) {
        my $entry_id = \$info->{entry_id};
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

    if( ! ref $local_filename) {
        $local_filename = Filesys::Filename->from_native( $local_filename );
    }
    $info->{filename} = $local_filename;
    return $info
}

sub delete_direntry( $self, $info ) {
    my $local_filename = $info->{filename};
    (my($mountpoint), $info->{filename}) = $self->to_alias( $info->{filename});

    my $res;
    if( defined $info->{entry_id}) {
        my $entry_id = \$info->{entry_id};
        my $tmp_res = $self->selectall_named(<<'SQL', $entry_id )->[0];
            delete from filesystem_entry
            where entry_id=:entry_id
            returning entry_id
SQL
        $res = $tmp_res->{entry_id};

    } else {
        # $info->{filename} must exist
        my $filename = $info->{filename};
        my $tmp_res = $self->selectall_named(<<'SQL', $mountpoint, $filename );
            delete from filesystem_entry
            where mountpoint=:mountpoint
              and filename=:filename
            returning entry_id
SQL
        $res = $tmp_res->[0]->{entry_id};
    };
    $info->{entry_id} = $res;
    $info->{filename} = $local_filename;
    return $info
}

sub insert_or_update_relation( $self, $info ) {
    my $value = encode_json( $info );
    my $res = $self->selectall_named(<<'SQL', $value );
        insert into filesystem_relation (relation_json)
        values (:value)
        on conflict(relation_type,parent_id,child_id) do
        update set relation_json = :value
        returning relation_id
SQL
    $info->{relation_id} = $res->{relation_id};
    return $info
}

sub find_memberships_by_type_child( $self, $collection_type, $_entry_id ) {
    my $entry_id = \$_entry_id;
    my $res = $self->selectall_named(<<'SQL', $collection_type, $entry_id );
      select c.collection_id
        from filesystem_membership m
        join filesystem_collection c on m.collection_id = c.collection_id
        where c.collection_type = :collection_type
          and entry_id          = :entry_id
SQL
    return $res
}

sub find_memberships_by_parent( $self, $collection_type, $_parent_id ) {
    my $parent_id = \$_parent_id;
    my $res = $self->selectall_named(<<'SQL', $collection_type, $parent_id );
      select c.collection_id
           , m.entry_id
        from filesystem_membership m
        join filesystem_collection c on m.collection_id = c.collection_id
        where c.collection_type = :collection_type
          and parent_id          = :parent_id
SQL
    return $res
}

sub insert_or_update_collection( $self, $info ) {
    my $value = encode_json( $info );
    my $collection_id = $info->{collection_id};

    my $res;
    if(     ! $collection_id
        and $info->{collection_type}
        and $info->{collection_type} eq 'directory' ) {
        $res = $self->selectall_named(<<'SQL', $collection_id, $value );
            insert into filesystem_collection (collection_id, collection_json)
            values (:collection_id, :value)
            on conflict(collection_type, parent_id) do
            update set collection_json = :value
            returning collection_id
SQL
    } else {

        $res = $self->selectall_named(<<'SQL', $collection_id, $value );
            insert into filesystem_collection (collection_id, collection_json)
            values (:collection_id, :value)
            on conflict(collection_id) do
            update set collection_json = :value
            returning collection_id
SQL
    }
    $info->{collection_id} = $res->[0]->{collection_id};
    return $info
}

sub find_collection( $self, $_collection_id ) {
    my $collection_id = \$_collection_id;
    my $res = $self->selectall_named(<<'SQL', $collection_id );
      select *
        from filesystem_collection
        where collection_id = :collection_id
SQL
    return $res->[0]
}

sub all_collections( $self, $collection_type ) {
    my $res = $self->selectall_named(<<'SQL', $collection_type );
      select *
        from filesystem_collection
        where collection_type = :collection_type
SQL
    return [ map { $self->_inflate_collection( $_ )} $res->@* ]
}

sub insert_or_update_membership( $self, $info ) {
    my $value = encode_json( $info );
    croak "Need a collection" unless $info->{collection_id};
    my $res = $self->selectall_named(<<'SQL', $value );
        insert into filesystem_membership (membership_json)
        values (:value)
        on conflict(collection_id, entry_id) do
        update set membership_json = :value
        returning collection_id, entry_id
SQL
    return $info
}

sub _inflate_filename( $self, $mountpoint, $filename ) {
    my $mp = $self->mountpoints->{$mountpoint};
    # Why do we need this? JSON->decode does not return UTF-8 strings?!

    croak "Unknown mountpoint '$mountpoint'"
        unless $mp;

    $filename = decode( 'UTF-8', $filename );

    $filename = join "/", $mp->{directory}->value, $filename;
    return Filesys::Filename->new( value => $filename, encoding => $mp->{encoding} )
}

sub _inflate_entry( $self, $entry ) {
    my $res = $self->json->decode( $entry->{entry_json} );
    $res->{filename} = $self->_inflate_filename( $res->{mountpoint}, $res->{filename});
    $res->{entry_id} = $entry->{entry_id};
    return $res
}

sub _inflate_collection( $self, $collection ) {
    my $res = decode_json( $collection->{collection_json} );
    $res->{collection_id} = $collection->{collection_id};
    return $res
}

sub _inflate_sth( $self, $sth ) {
    return map {
        $self->_inflate_entry( $_ );
    } @{ $sth->fetchall_arrayref( {} )}
}

# here, we take the path as primary key:
sub find_direntry_by_filename( $self, $filename ) {
    if( ! ref $filename ) {
        $filename = $self->decode_filename( $filename );
    }

    (my($mountpoint), $filename) = $self->to_alias($filename);

    my $fn = $filename->value;
    my $res = $self->selectall_named(<<'SQL', $fn, $mountpoint);
        select entry_json
             , entry_id
          from filesystem_entry
        where filename = :fn
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
       order by mtime desc
SQL
}

# No prototype since we want to capture the variables passed in:
sub entries {
    my( $self, $columns, $where ) = splice @_,0,3;
    return $self->entries_ex(columns => $columns, where => $where, lexicals => \@_, level => 2);
}

1;
