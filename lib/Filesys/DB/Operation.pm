package Filesys::DB::Operation;
use 5.020;
#use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';

use Carp 'croak';

use Filesys::TreeWalker 'scan_tree_bf';

sub basic_direntry_info( $ent, $context, $defaults ) {
    $context //= { stat => [stat($ent)] };
    return {
        %$defaults,
        filename => $ent,
        filesize => $context->{stat}->[7],
        mtime    => $context->{stat}->[9],
    }
}

sub keep_fs_entry( $store, $name ) {
    if( $name =~ m![/\\](?:(?:\.(git|cvs|config|DS_Store))|__MACOSX|Thumbs.db|\.tmp)\z!i) {
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

sub do_scan( %options ) {
    my $store = delete $options{ store } or croak "Need a store";
    my $directories = delete $options{ directories };
    my $dry_run = delete $options{ dry_run };
    my $status = delete $options{ status } // sub {};

    # Maybe we want to preseed with DB results so that we get unscanned directories
    # first, or empty directories ?!
    scan_tree_bf(
        wanted => sub($name) { keep_fs_entry($store, $name ) },
        queue => $directories,
        file => sub($file,$context) {
            my $info = $store->find_direntry_by_filename( $file );
            if( ! $info) {
                $info = basic_direntry_info($file,$context, { entry_type => 'file' });
                $info = do_update( $info );
            };

            if( ! $dry_run ) {
                $info = update_properties( $info, context => $context );

                # We also want to create a relation here, with our parent directory?!
                # We also want to create a collection here, with our parent directory?!
                # We have that information in context->{parent}
                if( defined $context->{parent}) {
                    # This should always exist since we scan and create directories
                    # before scanning and creating their contents
                    my $parent = $store->find_direntry_by_filename( $context->{parent});

                    #my $relation = $store->insert_or_update_relation({
                        #parent_id => $parent->{entry_id},
                        #child_id  => $info->{entry_id},
                        #relation_type => 'directory',
                    #});
                    my $collection = $store->insert_or_update_collection({
                        parent_id => $parent->{entry_id},
                        collection_type => 'directory',
                        title => $parent->{title} // basename($parent->{filename}),
                    });
                    my $membership = $store->insert_or_update_membership({
                        collection_id => $collection->{collection_id},
                        entry_id => $info->{entry_id},
                        position => undef,
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

            $status->( 'scan', $directory );
            return 1
        },
    );
}

1;
