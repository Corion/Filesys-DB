#!perl
use strict;
use 5.020;

#use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';
use PerlX::Maybe;

use Filesys::DB;
use Filesys::DB::Watcher;
use Filesys::DB::Operation;

use Carp 'croak';
use Getopt::Long;
use POSIX 'strftime';
use Encode 'encode', 'decode';

use JSON::Path;
use YAML 'LoadFile';

use File::Basename;

BEGIN {
$ENV{HOME} //= $ENV{USERPROFILE}; # to shut up Music::Tag complaining about undefined $ENV{HOME}
}

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

sub do_scan( $op, @directories ) {
    $op->do_scan(
        directories => \@directories,
    );
}

sub do_rescan( $op, @sql ) {
    @sql = '1=1' unless @sql;
    my $where = join " ", @sql;
    status( sprintf "% 8s | %s", 'rescan', $where);

    my %rescan_parents;

    scan_tree_db(
        file => sub( $info, $context ) {
            # do a liveness check? and potentially delete the file entry
            # also, have a dry-run option, just listing the files out of date?!
            if( ! -e $info->{filename}) {

                my $parents = $store->find_memberships_by_type_child( 'directory', $info->{entry_id} );
                use Data::Dumper;
                die Dumper $parents;
                #
                #for my $p ($parents->@*) {
                #    $rescan_parents{ $p->{parent_id } } = 1;
                #};

                do_delete({ filename => $info->{filename}});
                # This blows away all other data, like tags, etc. as well.
                # Maybe we would want to mark it as orphaned instead?!
                # We should also mark the parent for a content re-scan
                # so we pick up new arrivals/renames

            } else {
                if( ! $dry_run ) {
                    $info = update_properties( $info, force => 1, context => $context );
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
}

my $op = Filesys::DB::Operation->new(
    store => $store,
    dry_run => $dry_run,
    status => sub($action,$location) {
        status( sprintf "% 8s | %s", $action, $location );
    },
);

if( $action eq 'scan') {
    do_scan( $op, @ARGV );
} elsif( $action eq 'rescan' ) {
    do_rescan( $op, @ARGV );

} elsif ($action eq 'watch' ) {
    my $watcher = Filesys::DB::Watcher->new(store => $store);
    status( sprintf "% 8s | %s", 'idle', "");
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
        status( sprintf "% 8s | %s", 'idle', "");
    });
}

# [ ] discriminate between "empty title", "no title" and  "not checked for title"
#   - empty: ""
#   - no title: null
#   - not chexked: key does not exist, column value still null...
# [ ] add "ephemeral" or "auxiliary" file/entry type, for thumbnails and other
#     stuff that is generated of a different source file
# [ ] gradual updater that doesn't scan the filesystem but only scans for
#     missing properties - this is maybe a separate program?
# [ ] Make the skip list configurable, and/or hide them by a special entry
# use cases
# - [ ] loupilot album view
# - [ ] RSS generation?!
# - [ ] Image gallery/photostream
# [ ] Add handler for YAML and Markdown files, extracting (for example) the title
#     Also, for anything else containing frontmatter ...
# [ ] (Video) thumbnail generation? using ffmpeg
# [ ] Slide/pdf thumbnail generation? using ?!
# [ ] Randomly revisit entries to check they are up to date. This basically
#     means finding two relatively prime numbers, one the frequency and one the
#     maximum expected staleness, and scanning all files with
#               entry_id % (count/(frequency*staleness)) = time()/frequency
# [.] Watch for changes to mountpoints or stuff below them, and automatically
#     update the database from that - Win32::ChangeNotify and/or inotify2,
#     and/or File::ChangeNotify::Simple
#     - [x] This now exists for Win32
#     - [x] This now exists for other systems, but untested
# [ ] maybe also be "reactive" and issue events
#     based on those changes? Or, alternatively, have SQLite be such an event
#     queue, as we have timestamps and thus can even replay events
# [ ] store/queud fs events in a table ao other programs can catch up?! what for?
