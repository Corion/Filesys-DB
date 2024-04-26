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
use Filesys::DB::TermIO 'status', 'msg';

use Getopt::Long;
use POSIX 'strftime';
use Encode 'encode', 'decode';

use JSON::Path;
use YAML 'LoadFile';

use File::Basename;

use Filesys::DB::FTS::Tokenizer; # so that the tokenizer subroutine is found

BEGIN {
$ENV{HOME} //= $ENV{USERPROFILE}; # to shut up Music::Tag complaining about undefined $ENV{HOME}
}

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s'      => \my $mount_alias,
    'config|c=s'     => \my $config_file,
    'dsn|d=s'        => \my $dsn,
    'rescan|r'       => \my $rescan,
    'dry-run|n'      => \my $dry_run,
    'all'            => \my $scan_all_mountpoints,
    'watch'          => \my $watch_all_mountpoints,
    'force'          => \my $force,
);

my $action = $watch_all_mountpoints ? 'watch'
           : $rescan ? 'rescan'
           : 'scan';

$dsn //= 'dbi:SQLite:dbname=db/filesys-db.sqlite';

my $store = Filesys::DB->new(
   dbh => {
       dsn => $dsn,
   }
);
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
    push @ARGV, map { $store->mountpoints->{ $_ }->{directory}->native } sort keys %{$store->mountpoints}
}

# We want a breadth-first FS scan, preferring the most recent entries
# over older entries (as we assume that old entries don't change much)

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

sub do_delete( $op, $info ) {
    $op->do_delete( $info );
};

sub do_scan( $op, @directories ) {
    $op->do_scan(
        directories => \@directories,
        force => $force,
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
            if( ! -e $info->{filename}->native) {

                my $parents = $store->find_memberships_by_type_child( 'directory', $info->{entry_id} );
                # we don't use this information yet
                for my $p ($parents->@*) {
                    $rescan_parents{ $p->{collection_id } } = 1;
                };

                do_delete($op, { filename => $info->{filename}});
                # This blows away all other data, like tags, etc. as well.
                # Maybe we would want to mark it as orphaned instead?!
                # We should also mark the parent for a content re-scan
                # so we pick up new arrivals/renames

            } else {

                if( ! keys %$context ) {
                    # We haven't hit the disk for the context, so fetch the
                    # basic context
                    $context = Filesys::TreeWalker::_collect_fs_info( $info->{filename} );
                };
                $info = $op->update_properties( $info, force => 1, context => $context );
            }
        },
        directory => sub( $info, $context ) {
            if( ! -e $info->{filename}->native) {
                do_delete($op, { filename => $info->{filename}});
            };
            return 1

        },
        where => $where,
    );

    if( keys %rescan_parents) {
        say "Need to rescan:";
        for (map { $store->find_collection( $_ ) } sort { $a <=> $b } keys %rescan_parents) {
            use Data::Dumper; say Dumper $_;
            say $_->{parent_id};
        }
    }

}

my $op = Filesys::DB::Operation->new(
    store => $store,
    dry_run => $dry_run,
    status => sub($action,$location) {
        status( sprintf "% 10s | %s", $action, $location );
    },
    msg => sub($str) {
        msg( sprintf "%s", $str );
    },
);

if( $action eq 'scan') {
    do_scan( $op, @ARGV );
} elsif( $action eq 'rescan' ) {
    do_rescan( $op, @ARGV );

} elsif ($action eq 'watch' ) {
    my $watcher = Filesys::DB::Watcher->new(store => $store);
    status( sprintf "% 10s | %s", 'idle', "");
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
            $info = $op->update_properties( $info, force => 1 );

        } elsif( $ev->{action} eq 'old_name' ) {
            # ignore this
        } elsif( $ev->{action} eq 'new_name' ) {
            # ignore this
        } elsif( $ev->{action} eq 'renamed' ) {
            # how should we handle renaming an item? Force a rescan to update other metadata?!
            my $info = $store->find_direntry_by_filename( $ev->{old_name});
            $info->{filename} = $ev->{new_name};
            $info = $op->update_properties( $info, force => 1 );
        }
        status( sprintf "% 10s | %s", 'idle', "");
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
# - [ ] RSS generation?! -> search
# - [ ] Image gallery/photostream
# [ ] Add handler for YAML and Markdown files, extracting (for example) the title
#     Also, for anything else containing frontmatter ...
# [ ] (Video) thumbnail generation? using ffmpeg
# [ ] Slide/pdf thumbnail generation? using ?!
# [ ] Randomly revisit entries to check they are up to date. This basically
#     means finding two relatively prime numbers, one the frequency and one the
#     maximum expected staleness, and scanning all files with
#               entry_id % (count/(frequency*staleness)) = time()/frequency
# [ ] maybe also be "reactive" and issue events
#     based on those changes? Or, alternatively, have SQLite be such an event
#     queue, as we have timestamps and thus can even replay events
# [ ] store/queud fs events in a table ao other programs can catch up?! what for?
# [ ] Have collections that are updated from queries, like the collections
#     per PDF creator, PDF company
