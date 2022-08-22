#!perl
use strict;
use 5.020;

use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';

use DBI ':sql_types';
use DBD::SQLite;
use lib '../Filesys-Scanner/lib';
use Carp 'croak';
use PadWalker 'var_name'; # for scope magic...
use DBIx::RunSQL;
use Getopt::Long;
use POSIX 'strftime';
use Encode 'encode', 'decode';

use JSON 'encode_json', 'decode_json';
use JSON::Path;

use Digest::SHA;
use MIME::Detect;

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
);

$mount_alias //= '${MOUNT}';
$mountpoint //= $ARGV[0];

# We start out by storing information about our music collection

my $dbh = DBI->connect('dbi:SQLite:dbname=db/filesys-db.sqlite', undef, undef, { RaiseError => 1, PrintError => 0 });

# We want a breadth-first FS scan, preferring the most recent entries
# over older entries (as we assume that old entries don't change much)

# We currently expect entries from a filesystem, not ftp/webdav/ssh yet
sub scan_tree_bf( %options ) {
    my $on_file      = delete $options{ file } // sub {};
    my $on_directory = delete $options{ directory } // sub {};
    my $wanted       = delete $options{ wanted } // sub { 1 };
    my $queue        = delete $options{ queue } // ['.'];

    state %statcache;

    if( $queue and ! ref $queue) {
        $queue = [$queue];
    };

    while (@$queue) {
        my $entry = shift @$queue;

        $entry =~ s![/\\]\z!!; # strip off any directory separator as we'll use our own

        my $stat = $statcache{$entry};

        if( -d $entry ) {
            my $stat = delete $statcache{ $entry };
            if( ! $on_directory->($entry, $stat)) {
                # we are actually not interested in this directory
                next;
            };

            opendir my $dh, $entry or croak "$entry: $!";
            my @entries = map {
                my $full = "$entry/$_";
                $statcache{ $full } = [stat($full)];
                $full
            } grep {
                    $_ ne '.'
                and $_ ne '..'
                and $wanted->("$entry/$_")
            } readdir $dh;

            @$queue = sort { $statcache{ $b }->[9] <=> $statcache{ $a }->[9] } @$queue, @entries;

        } elsif( -f $entry ) {
            # Conserve some memory
            my $stat = delete $statcache{ $entry };
            $on_file->($entry, $stat);

        } else {
            # we skip stuff that is neither a file nor a directory
        }
    }
};

sub basic_direntry_info( $ent, $stat, $defaults ) {
    return {
        %$defaults,
        filename => $ent,
        mtime    => $stat->[9],
        filesize => -s $ent,
    }
}

sub selectall_named {
    # No subroutine signature since we need to preserve the aliases in @_
    my( $dbh, $sql ) = splice @_, 0, 2;

    # Gather the names of the variables used in the routine calling us
    my %parameters = map {
        var_name(1, \$_) => $_
    } @_;

    my $sth = $dbh->prepare($sql);
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

# here, we take the path as primary key:
sub insert_or_update_direntry( $info, $mp=$mountpoint, $alias=$mount_alias ) {
    my $local_filename = $info->{filename};
    $info->{filename} =~ s!^\Q$mp\E!$alias!;
    my $value = encode_json( $info );

    # Clean out all values that should not be stored:
    delete @{$info}{ (grep { /^_temp/ } keys %$info) };

    my $res;
    if( defined $info->{entry_id}) {
        my $entry_id = $info->{entry_id};
        my $tmp_res = selectall_named($dbh, <<'SQL', $value, $entry_id )->[0];
            insert into filesystem_entry (entry_id, entry_json)
            values (:entry_id, :value)
            on conflict(entry_id) do
            update set entry_json = :value
            returning entry_id
SQL
        $res = $tmp_res->{entry_id};

    } else {
        # $info->{filename} must be unique
        my $tmp_res = selectall_named($dbh, <<'SQL', $value );
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
sub find_direntry_by_filename( $filename, $mp=$mountpoint, $alias=$mount_alias ) {
    $filename =~ s!^\Q$mp\E!$alias!;

    # All filenames will be UTF-8 encoded:
    $filename = encode('UTF-8', $filename );

    my $entry = selectall_named( $dbh, <<'SQL', $filename);
        select entry_json
             , entry_id
          from filesystem_entry
        where filename = :filename
SQL
    my $res;
    if( @$entry ) {
        $res = decode_json( $entry->[0]->{entry_json} );
        $res->{filename} = decode( 'UTF-8', $res->{filename}); # really?!
        $res->{filename} =~ s!^\Q${alias}\E!$mp!;
        $res->{entry_id} = $entry->[0]->{entry_id};
    };
    return $res
}

sub timestamp($ts=time) {
    return strftime '%Y-%m-%dT%H:%M:%SZ', gmtime($ts)
}

# Maybe this should move into its own, tiny, tiny module?!
# or we should bring Term::Output::List up to date/onto CPAN
{
    my $last;
    sub status($msg) {
        local $|=1;
        my $rubout = "";
        if( $last ) {
            $rubout .= "\r" . (" " x length($last)) . "\r";
        };
        print $rubout.$msg."\r";
        $last = $msg;
    }

    sub msg($msg) {
        my $_last = $last;
        status("");
        say $msg;
        status($_last);
    }
    # erase any still active status message
    END { status(""); }
}

# This is the first set of property handlers
our %file_properties = (
    # '$.content.title' ?
    # '$.content.text' ?
    # '$.content.html' ?
    '$.sha256' => sub( $info ) {
        my $file = $info->{filename};
        if( $info->{entry_type} eq 'file' ) {
            my $digest = Digest::SHA->new(256);
            $digest->addfile($file);
            $info->{sha256} = $digest->hexdigest;
            return 1
        }
    },
    '$.mime_type' => sub( $info ) {
        state $mime = MIME::Detect->new();
        if( $info->{entry_type} eq 'file' ) {
            my @types = $mime->mime_types($info->{filename});
            if( @types ) {
                my $type = $types[0];
                $info->{mime_type} = $type->mime_type;
                return 1
            }
        }
    }
);

# Maybe we want to preseed with DB results so that we get unscanned directories
# first, or empty directories ?!
scan_tree_bf(
    wanted => sub($ent) { $ent !~ /\b(?:(?:\.(git|cvs|config))|__MACOSX|\.DS_Store)$/i },
    queue => \@ARGV,
    file => sub($file,$stat) {

        my $info = find_direntry_by_filename( $file );
        if( ! $info) {
            $info = basic_direntry_info($file,$stat,{ entry_type => 'file' });
            $info = insert_or_update_direntry($info, $mountpoint, $mount_alias);
        };
        my $last_ts = $info->{last_scanned} // '';

        # How do we find new columns added to basic_direntry_info ?!
        # Do we specify these all manually?!

        # This would be a kind of plugin system, maybe?!
        # Also, how will we handle a nested key like media.title ?! ( meaning ->{media}->{title} )
        state %path_cache;
        for my $prop (sort keys %file_properties) {
            $path_cache{ $prop } //= JSON::Path->new( $prop );
            if( ! defined $path_cache{ $prop }->value($info)) {
                status( sprintf "% 16s | %s", $prop, $info->{filename});
                if( $file_properties{$prop}->($info)) {
                    #msg("$info->{filename} - Last scan updated to '$info->{last_scanned}'");
                    $info->{last_scanned} = timestamp;
                };
            };
        };

        # the same for other fields:
        # If we changed anything, update the database:

        if( $info->{last_scanned} ne $last_ts ) {
            #msg( sprintf "% 16s | %s", 'update', $file);
            $info = insert_or_update_direntry($info);
            #use Data::Dumper; msg( Dumper $info );
        }

        # We also want to create a relation here, with our parent directory?!

    },
    directory => sub( $directory, $stat ) {
        my $info = find_direntry_by_filename( $directory );
        if( ! $info ) {
            $info = basic_direntry_info($directory,$stat,{ entry_type => 'directory' });
            #status( "-- %s (%d)", $directory, insert_or_update_direntry($info)->{entry_id} );
            $info = insert_or_update_direntry($info);
        };

        status( sprintf "% 16s | %s", 'scan', $directory);
        return 1
    },
);

# The query backend should become a separate script, later on

# [ ] add content.title column
# [ ] add media.duration column
# [ ] add "ephemeral" or "auxiliary" file/entry type, for thumbnails and other
#     stuff that is generated of a different source file
