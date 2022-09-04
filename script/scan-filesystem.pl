#!perl
use strict;
use 5.020;

use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';
use PerlX::Maybe;

use Filesys::DB;
use Carp 'croak';
use Getopt::Long;
use POSIX 'strftime';
use Encode 'encode', 'decode';

use JSON::Path;
use YAML 'LoadFile';

use File::Basename;

use Digest::SHA;
use MIME::Detect;
use Music::Tag 'traditional' => 1;

use lib '../Apache-Tika-Async/lib';
use Apache::Tika::Server;

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s'      => \my $mount_alias,
    'config|c=s'     => \my $config_file,
    'rescan|r'       => \my $rescan,
);

my $config = {};
my $user_config = {};
if(! defined $config_file ) {
    if ( 'filesys-db.yaml' ) {
        $config_file = 'filesys-db.yaml';
    } else {
        $user_config = {
            mountpoints => [
                {
                  alias => $mount_alias // '${MOUNT}',
                  directory => $mountpoint //  $ARGV[0],
                }
            ],
        }
    }
}
if( -f $config_file ) {
    $user_config = LoadFile( $config_file );
};
$user_config->{mountpoints} //= {};
$config->{mountpoints} = $user_config->{mountpoints};

my $store = Filesys::DB->new(
    mountpoints => {
        %{ $config->{mountpoints} },
        maybe $mount_alias => $mountpoint,
    },
);

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

sub audio_info( $audiofile, $artist=undef, $album=undef ) {
    # Maybe this can take over MP3 too?
    local $MP3::Info::try_harder = 1;
    my $tag = Music::Tag->new( $audiofile);

    $tag->get_tag;

    # Mush 03/10 into 03
    if( $tag->track =~ m!(\d+)\s*/\s*\d+$! ) {
        $tag->track( $1 );
    };

    my %info = map { $_ => $tag->$_() } qw(artist album track title duration);
    $info{ duration } ||= '-1000'; # "unknown" if we didn't find anything
    $audiofile =~ /\.(\w+)$/;
    $info{ ext } = lc $1;

    $info{ url } = basename( $audiofile ); # we assume the playlist will live in the same directory
    $info{ artist } //= $artist;
    $info{ album  } //= $album;
    $info{ track  } = sprintf '%02d', $info{ track };

    return \%info;
}

sub _mime_match( $pattern, $type ) {
    my $p = $pattern =~ s/\*/.*/r;
    return $type =~ qr!\A$p\z!
}

# This is the first set of property handlers
# This should/could be restructured to mime_type -> field maybe?!
# Or field -> mime_type ?
# We can recognize this and recursively descend?!

sub _applicable_properties( $props, $info, $options, $visual='???' ) {
    state %path_cache;
    my @res;
    if( ref $props eq 'HASH' ) {
        for my $prop (sort keys %$props) {
            if( $prop =~ m!^\$! ) {
                my $vis;
                if(  $prop =~ m!\.([^.]+)$! ) {
                    $vis = $1
                };

                # JSON path
                # Check that it is missing or we are rebuilding
                $path_cache{ $prop } //= JSON::Path->new( $prop );
                my $do_update = $options->{ force }
                            || ! defined $path_cache{ $prop }->value($info);
                if( $do_update ) {
                    push @res, _applicable_properties( $props->{$prop}, $info, $options, $vis );
                }

            } elsif( $prop =~ m!^[-*\w]+/[-*\w]+\z! ) {
                # MIME type
                # Check that it applies (or is empty?!)
                if( _mime_match( $prop, $info->{mime_type} )) {
                    push @res, _applicable_properties( $props->{$prop}, $info, $options, $visual );
                }
            } else {
                croak "Unknown property spec '$prop'";
            }
        }
    } elsif ( ref $props eq 'CODE' ) {
        push @res, [$visual, $props]
    } else {
        croak "Unknown property spec '$props'";
    }
    return @res
}

sub extract_content_via_tika( $info ) {
    my $filename = $info->{filename};

    state $tika //= do {
        my $t = Apache::Tika::Server->new(
            jarfile => '/home/corion/Projekte/Apache-Tika-Async/jar/tika-server-standard-2.3.0.jar',
        );
        $t->launch;
        $t
    };
    my $pdf_info = $tika->get_all( $filename );
    if( $pdf_info->meta->{'meta:language'} =~ /^(de|en|fr|zh)$/ ) {
        # I don't expect other languages, except for misdetections
        $info->{language} = $pdf_info->meta->{'meta:language'};
    }
    $info->{content}->{title} = $pdf_info->meta->{'dc:title'};
    $info->{content}->{html} = $pdf_info->content();

    return 1;
}

sub extract_content_via_audio_tag( $info ) {
    return if $info->{mime_type} eq 'audio/x-mpegurl';
    return if $info->{mime_type} eq 'audio/x-scpls';
    return if $info->{mime_type} eq 'audio/x-wav';

    my $res;

    my $audio_info = audio_info( $info->{filename} );
    for( qw(title artist album track duration)) {
        if( ! defined $info->{content}->{$_}) {
            $info->{content}->{$_} = $audio_info->{$_};
            $res = 1;
        }
    };
    $res;
}

our %file_properties = (
    # '$.content.text' ?
    '$.mountpoint' => sub( $info ) {
        $info->{mountpoint} = $store->get_mountpoint_alias( $info->{filename});
        1
    },
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
    },
    '$.content.title' => {
        'audio/*' => \&extract_content_via_audio_tag,
    },
    'application/pdf' => {
        # Arrayref here, so we only make a single call?!
        '$.content.title' => \&extract_content_via_tika,
        '$.content.html'  => \&extract_content_via_tika,
    },
);

sub keep_fs_entry( $name ) {
    if( $name =~ m![/\\](?:(?:\.(git|cvs|config|DS_Store))|__MACOSX|Thumbs.db)\z!i) {
        msg("Skipping '$name'");
        return undef
    }

    my ($mp,$fn) = $store->to_alias( $name );
    my $skip = $store->mountpoints->{$mp};
    if( grep { index( $_, $name ) == 0 } @{ $skip->{'skip-index'} || []}) {
        msg("Skipping '$name'");
        return undef
    }

    1
}

sub update_properties( $info, %options ) {
    my $last_ts = $info->{last_scanned} // '';

    # How do we find new columns added to basic_direntry_info ?!
    # Do we specify these all manually?!

    # This would be a kind of plugin system, maybe?!
    # Also, how will we handle a nested key like media.title ?! ( meaning ->{media}->{title} )
    #state %path_cache;
    #for my $prop (sort keys %file_properties) {
    my @updaters = _applicable_properties( \%file_properties, $info, \%options );
    for my $up (@updaters) {
        my( $vis, $cb ) = @$up;
        status( sprintf "% 16s | %s", $vis, $info->{filename});
        if( $cb->($info)) {
            $info->{last_scanned} = timestamp;
        };
    };

    # the same for other fields:
    # If we changed anything, update the database:

    if( $info->{last_scanned} ne $last_ts ) {
        #msg( sprintf "% 16s | %s", 'update', $file);
        $info = $store->insert_or_update_direntry($info);
    }
    return $info
}

sub scan_tree_db( %options ) {
    $options{ level } //= 0;
    $options{ level } += 1;
    my @entries = $store->_inflate_sth( $store->entries_ex(%options));
    scan_entries(
        file => $options{ file },
        dir  => $options{ dir },
        entries => \@entries,
        # wanted has already happened
        # queue does not exist
    )
}

sub scan_entries( %options ) {
    for my $entry (@{$options{entries}}) {
        if( $entry->{entry_type} eq 'file' ) {
            $options{ file }->( $entry, undef );
        } elsif( $entry->{entry_type} eq 'directory' ) {
            $options{ dir }->( $entry, undef );
        }
    }
}

if( ! $rescan) {
    # Maybe we want to preseed with DB results so that we get unscanned directories
    # first, or empty directories ?!
    scan_tree_bf(
        wanted => \&keep_fs_entry,
        queue => \@ARGV,
        file => sub($file,$stat) {

            my $info = $store->find_direntry_by_filename( $file );
            if( ! $info) {
                $info = basic_direntry_info($file,$stat,{ entry_type => 'file' });
                $info = $store->insert_or_update_direntry($info);
            };
            $info = update_properties( $info );

            # We also want to create a relation here, with our parent directory?!
        },
        directory => sub( $directory, $stat ) {
            my $info = $store->find_direntry_by_filename( $directory );
            if( ! $info ) {
                $info = basic_direntry_info($directory,$stat,{ entry_type => 'directory' });
                #status( "-- %s (%d)", $directory, insert_or_update_direntry($info)->{entry_id} );
                $info = $store->insert_or_update_direntry($info);
            };

            status( sprintf "% 16s | %s", 'scan', $directory);
            return 1
        },
    );
} else {
    my $where = join " ", @ARGV;
    status( sprintf "% 16s | %s", 'rescan', $where);
    scan_tree_db(
        file => sub( $info, $stat ) {
            # do a liveness check? and potentially delete the file entry
            # also, have a dry-run option, just listing the files out of date?!
            $info = update_properties( $info, force => 1 );
        },
        directory => sub( $info, $stat ) {
            return 1
        },
        where => $where,
    );
}

# [ ] add "ephemeral" or "auxiliary" file/entry type, for thumbnails and other
#     stuff that is generated of a different source file
# [ ] read mountpoints config from YAML
# [ ] gradual updater that doesn't scan the filesystem but only scans for
#     missing properties - this is maybe a separate program?
# [ ] Make the skip list configurable, and/or hide them by a special entry
# [ ] Add handler for YAML and Markdown files, extracting (for example) the title
#     Also, for anything else containing frontmatter ...
