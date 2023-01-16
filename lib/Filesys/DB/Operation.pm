package Filesys::DB::Operation;
use 5.020;
#use Filter::signatures;
use Moo 2;
use feature 'signatures';
no warnings 'experimental::signatures';
use Scalar::Util 'weaken';
use File::Basename;

use Carp 'croak';

use Filesys::TreeWalker 'scan_tree_bf';

# For the content scanner
use Digest::SHA;
use MIME::Detect;
use Music::Tag 'traditional' => 1;
use Apache::Tika::Server;
use POSIX 'strftime';

has 'store' => (
    is => 'ro',
);

has 'dry_run' => (
    is => 'ro',
);

has 'status' => (
    is => 'ro',
    default => sub { sub {} },
);

has 'msg' => (
    is => 'ro',
    default => sub { sub {} },
);

sub timestamp($ts=time) {
    return strftime '%Y-%m-%dT%H:%M:%SZ', gmtime($ts)
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
                    if( $info->{mime_type} && _mime_match( $prop, $info->{mime_type} )) {
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

sub extract_content_via_tika( $self, $info ) {
    my $filename = $info->{filename};

    state $tika //= do {
        my $t = eval {
                    Apache::Tika::Server->new(
            jarfile => '/home/corion/Projekte/Apache-Tika-Async/jar/tika-server-standard-2.3.0.jar',
            );};
        eval { $t->launch; };
        ! $@ and $t
    };
    if($tika) {
        my $pdf_info = $tika->get_all( $filename );
        if( $pdf_info->meta->{'meta:language'} =~ /^(de|en|fr|zh)$/ ) {
            # I don't expect other languages, except for misdetections
            $info->{language} = $pdf_info->meta->{'meta:language'};
        }
        $info->{content}->{title} = $pdf_info->meta->{'dc:title'};
        $info->{content}->{html} = $pdf_info->content();

        return 1;
    } else { return 0 }
}

sub extract_content_via_audio_tag( $self, $info ) {
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
    '$.mountpoint' => sub( $self, $info ) {
        $info->{mountpoint} = $self->store->get_mountpoint_alias( $info->{filename});
        1
    },
    '$.sha256' => sub( $self, $info ) {
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
    '$.mime_type' => sub( $self, $info ) {
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

sub update_properties( $self, $info, %options ) {
    my $last_ts = $info->{last_scanned} // '';

    # This would be a kind of plugin system, maybe?!
    my $do_scan;

    if( exists $options{ context }) {
        if( ! $info->{last_scanned} ) {
            $do_scan = 1
        } else {
            $do_scan = timestamp($options{ context }->{stat}->[9]) gt $last_ts;
        };
    };

    if( $do_scan ) {
        my @updaters = _applicable_properties( \%file_properties, $info, \%options );
        for my $up (@updaters) {
            my( $vis, $cb ) = @$up;
            my $status = $self->status;
            $status->( $vis, $info->{filename});
            if( $cb->($self, $info)) {
                $info->{last_scanned} = timestamp;
            };
        };
    }

    # the same for other fields:
    # If we changed anything, update the database:

    if(( $info->{last_scanned} // '' ) ne $last_ts ) {
        #msg( sprintf "% 8s | %s", 'update', $file);
        $info = $self->do_update($info);
    }
    return $info
}

sub basic_direntry_info( $self, $ent, $context, $defaults ) {
    $context //= { stat => [stat($ent)] };
    return {
        %$defaults,
        filename => $ent,
        mtime    => $context->{stat}->[9],
    }
}

sub keep_fs_entry( $self, $name ) {
    my $store = $self->store;

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

sub do_scan( $self, %options ) {
    my $store = $self->store;
    my $directories = delete $options{ directories };
    my $dry_run = delete $options{ dry_run };
    my $status = $self->status;
    my $msg    = $self->msg;

    weaken(my $s = $self);

    # Maybe we want to preseed with DB results so that we get unscanned directories
    # first, or empty directories ?!
    scan_tree_bf(
        wanted => sub($name) { $s->keep_fs_entry($name ) },
        queue => $directories,
        file => sub($file,$context) {
            my $info = $store->find_direntry_by_filename( $file );
            if( ! $info) {
                $info = $self->basic_direntry_info($file,$context, { entry_type => 'file' });
                $info = $self->do_update( $info );
            };

            if( ! $dry_run ) {
                $info = $self->update_properties( $info, context => $context );

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
                $info = $self->basic_direntry_info($directory,$context,{ entry_type => 'directory' });
                $info = $self->do_update(
                    $info,
                );
            };

            $status->( 'scan', $directory );
            return 1
        },
    );
}

sub do_update( $self, $info, %options ) {
    my $dry_run = delete $options{ dry_run };
    if( $dry_run ) {
        $self->msg->( "update,$info->{filename}" );
        return $info;
    } else {
        $info = $self->store->insert_or_update_direntry($info);
    }
};

1;