#!perl
use strict;
use 5.020;

#use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';
use PerlX::Maybe;

use Filesys::DB;
use Filesys::DB::Watcher;

use Carp 'croak';
use Getopt::Long;
use POSIX 'strftime';
use Encode 'encode', 'decode';

use JSON::Path;
use YAML 'LoadFile';

use File::Basename;

use Digest::SHA;
use MIME::Detect;
BEGIN {
$ENV{HOME} //= $ENV{USERPROFILE}; # to shut up Music::Tag complaining about undefined $ENV{HOME}
}

use Music::Tag 'traditional' => 1;
use Apache::Tika::Server;

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s'      => \my $mount_alias,
    'config|c=s'     => \my $config_file,
    'rescan|r'       => \my $rescan,
    'dry-run|n'      => \my $dry_run,
    'all'            => \my $scan_all_mountpoints,
    'watch'          => \my $watch_all_mountpoints,
);

my $action = $watch_all_mountpoints ? 'watch'
           : $rescan ? 'rescan'
           : 'scan';

my $store = Filesys::DB->new();
$store->init_config(
    default_config_file => 'filesys-db.yaml',
    config_file         => $config_file,
);

if( $mount_alias and $mountpoint ) {
    $store->mountpoints->{ $mount_alias } = +{ directory => $mountpoint, alias => $mount_alias };
}

# Also, if we only have the alias, extract the base directory to scan from
# there:
if( $mount_alias && !@ARGV ) {
    my $mp = $store->mountpoints->{$mount_alias}
        or die "Unknown mount point '$mount_alias'";
    push @ARGV, $store->mountpoints->{$mount_alias}->{directory}
} elsif( $scan_all_mountpoints ) {
    push @ARGV, map { $store->mountpoints->{ $_ }->{directory}} sort keys %{$store->mountpoints}
}

# We want a breadth-first FS scan, preferring the most recent entries
# over older entries (as we assume that old entries don't change much)

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
            # Conserve some memory
            $on_file->($entry->{name}, $entry);

        } else {
            # we skip stuff that is neither a file nor a directory
        }
    }
};

sub basic_direntry_info( $ent, $context, $defaults ) {
    $context //= { stat => [stat($ent)] };
    return {
        %$defaults,
        filename => $ent,
        mtime    => $context->{stat}->[9],
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
    my $colcount;

    our $use_tput = `tput cols`;
    state $is_tty = -t STDOUT;

    sub get_colcount() {
        if( $use_tput ) {
            $SIG{WINCH} = sub {
                undef $colcount;
            };

            return 0+`tput cols`
        } elsif( $^O =~ /mswin/i ) {
            require Win32::Console;
            return [Win32::Console->new()->Size()]->[0]
        } else {
            return undef
        }
    }

    sub col_trunc($msg) {
        $colcount //= get_colcount();
        my $vis = $msg;
        if( length($msg) > $colcount ) {
             $vis = substr( $msg, 0, $colcount-4 ) . '...';
        }
        return $vis
    }

    sub status($msg) {
        return if ! $is_tty; # no status if nobody is watching
        local $|=1;
        my $rubout = "";
        if( $last ) {
            $rubout .= "\r" . col_trunc(" " x length($last)) . "\r";
        };
        my $vis = col_trunc($msg);
        print $rubout.$vis."\r";
        $last = $vis;
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

# This does a recursive descent to find whether rules apply or not
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
                    my @prop;
                    eval { @prop = _applicable_properties( $props->{$prop}, $info, $options, $vis ); };
                    if( $@ ) {
                        return
                    } else {
                        push @res, @prop
                    }
                }

            } elsif( $prop =~ m!^[-.*\w]+/[-.*\w]+\z! ) {
                # MIME type
                # Check that it applies (or is empty?!)
                eval {
                    if( _mime_match( $prop, $info->{mime_type} )) {
                        push @res, _applicable_properties( $props->{$prop}, $info, $options, $visual );
                    }
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
            eval {
                $digest->addfile($file);
                $info->{sha256} = $digest->hexdigest;
                return 1
            };
            return 0;
        }
    },
    '$.mime_type' => sub( $info ) {
        state $mime = MIME::Detect->new();
        if( $info->{entry_type} eq 'file' ) {

            my @types;
            eval { @types = $mime->mime_types($info->{filename}); };
            if( $@ ) {
                return 0;
            };
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
    # Arrayref here, so we only make a single call?!
    'application/vnd.oasis.opendocument.presentation' => {
        '$.content.title' => \&extract_content_via_tika,
        '$.content.html'  => \&extract_content_via_tika,
    },
    'application/pdf' => {
        # Arrayref here, so we only make a single call?!
        '$.content.title' => \&extract_content_via_tika,
        '$.content.html'  => \&extract_content_via_tika,
    },
);

sub keep_fs_entry( $name ) {
    if( $name =~ m![/\\](?:(?:\.(git|cvs|config|DS_Store))|__MACOSX|Thumbs.db)\z!i) {
        # msg("Skipping '$name'");
        return undef
    }

    my ($mp,$fn) = $store->to_alias( $name );
    my $skip = $store->mountpoints->{$mp};
    if( grep { index( $_, $name ) == 0 } @{ $skip->{'skip-index'} || []}) {
        # msg("Skipping '$name'");
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
        $info = do_update($info);
    }
    return $info
}

sub scan_tree_db( %options ) {
    $options{ level } //= 0;
    $options{ level } += 1;
    my @entries = $store->_inflate_sth( $store->entries_ex(%options));
    scan_entries(
        file      => $options{ file },
        directory => $options{ directory },
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
            $options{ directory }->( $entry, undef );
        }
    }
}

sub do_delete( $info ) {
    if( $dry_run ) {
        msg( "delete,$info->{filename}" );
    } else {
        $store->delete_direntry($info);
    }
};

sub do_update( $info, %options ) {
    if( $dry_run ) {
        msg( "update,$info->{filename}" );
        return $info;
    } else {
        $info = $store->insert_or_update_direntry($info);
    }
};

if( $action eq 'scan') {
    # Maybe we want to preseed with DB results so that we get unscanned directories
    # first, or empty directories ?!
    scan_tree_bf(
        wanted => \&keep_fs_entry,
        queue => \@ARGV,
        file => sub($file,$context) {
            my $info = $store->find_direntry_by_filename( $file );
            if( ! $info) {
                $info = basic_direntry_info($file,$context, { entry_type => 'file' });
                $info = do_update( $info );
            };
            
            if( ! $dry_run ) {
                $info = update_properties( $info );
    
                # We also want to create a relation here, with our parent directory?!
                # We have that information in context->{parent}
                if( defined $context->{parent}) {
                    # This should always exist since we scan and create directories
                    # before scanning and creating their contents
                    my $parent = $store->find_direntry_by_filename( $context->{parent});
    
                    my $relation = $store->insert_or_update_relation({
                        parent_id => $parent->{entry_id},
                        child_id  => $info->{entry_id},
                        relation_type => 'directory',
                    });
                }
            }

        },
        directory => sub( $directory, $context ) {
            my $info = $store->find_direntry_by_filename( $directory );
            if( ! $info ) {
                $info = basic_direntry_info($directory,$context,{ entry_type => 'directory' });
                $info = do_update($info);
            };

            status( sprintf "% 16s | %s", 'scan', $directory);
            return 1
        },
    );
} elsif( $action eq 'rescan' ) {
    @ARGV = '1=1' unless @ARGV;
    my $where = join " ", @ARGV;
    status( sprintf "% 16s | %s", 'rescan', $where);
    scan_tree_db(
        file => sub( $info, $context ) {
            # do a liveness check? and potentially delete the file entry
            # also, have a dry-run option, just listing the files out of date?!
            if( ! -e $info->{filename}) {
                # The file has gone away
                do_delete({ filename => $info->{filename}});
                # This blows away all other data, like tags, etc. as well.
                # Maybe we would want to mark it as orphaned instead?!
            } else {
                if( ! $dry_run ) {
                    $info = update_properties( $info, force => 1 );
                };
            }
        },
        directory => sub( $info, $context ) {
            if( ! -e $info->{filename}) {
                do_delete({ filename => $info->{filename}});
            };
            return 1
            
        },
        where => $where,
    );
} elsif ($action eq 'watch' ) {
    my $watcher = Filesys::DB::Watcher->new(store => $store);
    status( sprintf "% 16s | %s", 'idle', "");
    # Can we / do we want to debounce this? While a file is copied, we will
    # also start to scan it, which is not great. But waiting a second for things to
    # settle down also means some async behaviour, which isn't great either
    $watcher->watch(cb => sub($ev) {
        my $file = $ev->{path};
        if( $ev->{action} eq 'added') {
            my $info = $store->find_direntry_by_filename( $file );
            if( -f $file ) {
                if( ! $info) {
                    $info = basic_direntry_info($file, undef, { entry_type => 'file' });
                };
            } elsif( -d $file ) {
                if( ! $info) {
                    $info = basic_direntry_info($file, undef, { entry_type => 'directory' });
                };
            }
            $info = do_update($info);

        } elsif( $ev->{action} eq 'removed') {
            # we should remove the file from the DB
            my $info = $store->delete_direntry({ filename => $ev->{path}});

        } elsif( $ev->{action} eq 'modified' ) {
            # we should update (or remove?) our metadata
            # but starting right now will mean outdated information?!
            # This will skip/ignore zero-size files...
            return unless -s $file;
            
            my $info = $store->find_direntry_by_filename( $file );
            $info = update_properties( $info, force => 1 );

        } elsif( $ev->{action} eq 'old_name' ) {
            # ignore this
        } elsif( $ev->{action} eq 'new_name' ) {
            # ignore this
        } elsif( $ev->{action} eq 'renamed' ) {
            # how should we handle renaming an item? Force a rescan to update other metadata?!
            my $info = $store->find_direntry_by_filename( $ev->{old_name});
            $info->{filename} = $ev->{new_name};
            $info = update_properties( $info, force => 1 );
        }
        status( sprintf "% 16s | %s", 'idle', "");
    });
}

# [ ] add "ephemeral" or "auxiliary" file/entry type, for thumbnails and other
#     stuff that is generated of a different source file
# [ ] gradual updater that doesn't scan the filesystem but only scans for
#     missing properties - this is maybe a separate program?
# [ ] Make the skip list configurable, and/or hide them by a special entry
# [ ] Add handler for YAML and Markdown files, extracting (for example) the title
#     Also, for anything else containing frontmatter ...
# [ ] (Video) thumbnail generation? using ffmpeg
# [ ] Slide/pdf thumbnail generation? using ?!
# [ ] Randomly revisit entries to check they are up to date. This basically
#     means finding two relatively prime numbers, one the frequency and one the
#     maximum expected staleness, and scanning all files with
#               entry_id % (count/(frequency*staleness)) = time()/frequency
# [ ] Watch for changes to mountpoints or stuff below them, and automatically
#     update the database from that - Win32::ChangeNotify and/or inotify2,
#     and/or File::ChangeNotify::Simple
#     - [x] This now exists for Win32
# [ ] maybe also be "reactive" and issue events
#     based on those changes? Or, alternatively, have SQLite be such an event
#     queue, as we have timestamps and thus can even replay events