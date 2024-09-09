package Filesys::DB::Operation;
use 5.020;
use Moo 2;
use experimental 'signatures';

our $VERSION = '0.01';

use Scalar::Util 'weaken';
use File::Basename;

use Carp 'croak';

use Filesys::TreeWalker 'scan_tree_bf';

# For the content scanner
use JSON::Path;
use Digest::SHA;
use MIME::Detect;
use Music::Tag 'traditional' => 1;
use Music::Tag::MP3;
use lib '../Apache-Tika-Async/lib';
use Apache::Tika::Server;
use POSIX 'strftime';
use Encode 'encode', 'decode';
use Filesys::Filename;

use Text::FrontMatter::YAML;

has 'store' => (
    is => 'ro',
);

has 'dry_run' => (
    is => 'ro',
);

has 'cutoff' => (
    is => 'rw',
    default => sub { 0 },
);

has 'level' => (
    is => 'rw',
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
    my $tr = $tag->track;
    if( $tr and $tr =~ m!(\d+)\s*/\s*\d+$! ) {
        $tag->track( $1 );
    };

    my %info = map { $_ => $tag->$_() } qw(artist album track title duration year);
    $info{ duration } ||= '-1000'; # "unknown" if we didn't find anything
    $audiofile =~ /\.(\w+)$/;
    $info{ ext } = lc $1;

    $info{ url } = basename( $audiofile ); # we assume the playlist will live in the same directory
    $info{ artist } //= $artist;
    $info{ album  } //= $album;
    if( defined $info{ track } ) {
        $info{ track  } = sprintf '%02d', $info{ track };
    };

    if( defined $info{ year } ) {
        $info{ year } = sprintf '%04d', $info{ year };
    };

    if( my $tag = $tag->plugin('MP3') ) {
        if( my $mp3 = $tag->{ID3v2} ) {
            if( my $bpm = $mp3->get_frame("TBPM")) {
                $info{ bpm } = $bpm;
            }
            if( my $release = $mp3->get_frame("TDRL")) {
                $info{ year } = $release;
            }
        }
    }

    return \%info;
}

sub _mime_match( $pattern, $type ) {
    my $p = $pattern =~ s/\*/.*/r;
    return $type =~ qr!\A$p\z!
}

use constant EXISTS_BUT_EMPTY => "\0undefined";

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
                # XXX Meh - we want to differentiate here between a path that does not exist
                #     -> rescan
                #     vs a path that does exist but is undef
                #     -> already checked, no need to rescan because it does not have a value
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

sub changed( $r_old, $new ) {
    my $changed = ( $$r_old // '' ) ne ($new // '')
                ? 1
                : 0;
    $$r_old = $new;
    $changed
}

sub extract_content_via_tika( $self, $info ) {
    my $filename = $info->{filename}->native;
    state $tika //= do {
        my $t = eval {

            my $file = Apache::Tika::Async->best_jar_file(
                glob '../Apache-Tika-Async/jar/*.jar'
            );
            $file = File::Spec->rel2abs($file, '.');

			# YOu can set the environment to specify a custom Tika path
			# or Tika jar file
            Apache::Tika::Server->new(
                jarfile => $file,
            );
        };
        eval { $t->launch; };
        $self->msg->( $@ ) if $@;
        sleep 5;
        ! $@ and $t
    };

    if($tika) {
        my $changed = 0;
        my $pdf_info = eval { $tika->get_all( $filename ); };

        if( my $err = $@ ) {
            $tika = 0;
            return;
        };

        my $lang =    $pdf_info->meta->{'dc:language'}
                   // $pdf_info->meta->{'meta:language'}
                   // 'en';
        $lang = lc $lang;
        $lang =~ s/-\w+$//; # en-gb -> en , even though we lose a tiny bit here
        $lang = 'en' if $lang eq 'th'; # weird misdetection

        #if( $lang =~ /^(de|en|fr|zh)$/ ) {
            # I don't expect other languages, except for misdetections
            # Shouldn't this be ->{content}->{language} ?!
            $changed += changed( \($info->{language}), $lang);
        #}
        $changed += changed( \($info->{content}->{title}), $pdf_info->meta->{'dc:title'} // EXISTS_BUT_EMPTY );
        $changed += changed( \($info->{content}->{creator}), $pdf_info->meta->{'dc:creator'} // EXISTS_BUT_EMPTY );
        $changed += changed( \($info->{content}->{company}), $pdf_info->meta->{'pdf:docinfo:custom:Company'} // EXISTS_BUT_EMPTY );
        $changed += changed( \($info->{content}->{html}), $pdf_info->content() // EXISTS_BUT_EMPTY );

        return $changed;
    } else {
        return 0
    }
}

sub extract_content_via_audio_tag( $self, $info ) {
    return if $info->{mime_type} eq 'audio/x-mpegurl';
    return if $info->{mime_type} eq 'audio/x-scpls';
    return if $info->{mime_type} eq 'audio/x-wav';

    my $changed;

    my $audio_info = audio_info( $info->{filename}->native );
    for( qw(title artist album track duration year date)) {
        if( ! defined $info->{content}->{$_}) {
            $changed += changed( \($info->{content}->{$_}), $audio_info->{$_});
        }
    };
    return $changed;
}

sub extract_content_from_markdown( $self, $info ) {
    open my $fh, '<:raw', $info->{filename}->native
        or return;
    my $content = do {
        local (@ARGV, $/) = $info->{filename}->native;
        <>
    };

    my $changed = 0;
    eval {
        my $tfm = Text::FrontMatter::YAML->new(
            document_string => $content
        );
        my $frontmatter = $tfm->frontmatter_hashref;
        $changed += changed( \($info->{content}->{title}),   $frontmatter->{'title'} // EXISTS_BUT_EMPTY );
        $changed += changed( \($info->{content}->{creator}), $frontmatter->{'author'} // EXISTS_BUT_EMPTY );
        $changed += changed( \($info->{language}), $frontmatter->{'language'} );
        $changed += changed( \($info->{content}->{html}),    $tfm->data_text() // EXISTS_BUT_EMPTY );
    };

    return $changed
}

our %file_properties = (
    # '$.content.text' ?
    '$.mountpoint' => sub( $self, $info ) {
        # Only update if we have an absolute filename and no mountpoint?!
        if( $info->{filename}->value =~ m!/!
            and ! $info->{mountpoint} ) {
            $info->{mountpoint} = $self->store->get_mountpoint_alias( $info->{filename});
        };
        0
    },
    '$.sha256' => sub( $self, $info ) {
        my $file = $info->{filename}->native;
        if( $info->{entry_type} eq 'file' ) {
            my $digest = Digest::SHA->new(256);
            eval {
                $digest->addfile($file);
                my $old = $info->{sha256};
                $info->{sha256} = $digest->hexdigest;
                return (($old || '') ne $info->{sha256})
            };
            return 0;
        }
    },
    '$.mime_type' => sub( $self, $info ) {
        state $mime = MIME::Detect->new();
        if( $info->{entry_type} eq 'file' ) {

            my @types;
            eval { @types = $mime->mime_types($info->{filename}->native); };
            if( $@ ) {
                return 0;
            };
            if( @types ) {
                my $type = $types[0];
                my $old = $info->{mime_type} // '';
                $info->{mime_type} = $type->mime_type;
                return $old ne $info->{mime_type}
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
        '$.content.creator'  => \&extract_content_via_tika,
    },
    'application/pdf' => {
        # Arrayref here, so we only make a single call?!
        '$.content.title' => \&extract_content_via_tika,
        '$.content.html'  => \&extract_content_via_tika,
        '$.content.creator'  => \&extract_content_via_tika,
        '$.content.company'  => \&extract_content_via_tika,
    },
    'text/plain' => { # we assume it's markdown ...
        '$.content.title' => \&extract_content_from_markdown,
        '$.content.html'  => \&extract_content_from_markdown,
        '$.content.creator'  => \&extract_content_from_markdown,
     },
);

sub _wants_rescan( $self, $info, $options ) {
    return 1 if $options->{force};

    if( ! exists $options->{context}) {
        $options->{context} = {};
    };
    if( ! exists $options->{context}->{stat} ) {
        my $fn;
        if( $info->{mountpoint}) {
            $fn = $self->store->_inflate_filename( $info->{mountpoint}, $info->{filename} );
        } else {
            $fn = $info->{filename};
        };
        $options->{context}->{stat} = [stat $fn->native];
    };

    my ($do_scan, $reason);
    if( ! $info->{last_scanned} ) {
        # This should be guarded by a verbosity/log level thing...
        #$self->msg->("rescan,no_info,$info->{filename}");
        $reason = 'Never scanned';
        $do_scan = 1;

    } else {
        # Check last change timestamp of the file
        # and filesize. If either changed, we should rescan
        if(     $options->{ context }->{stat}
            and $options->{ context }->{stat}->@* ) {
            my $ts = timestamp($options->{ context }->{stat}->[9]);
            my $last_ts = $options->{ last_ts } // $info->{last_scanned};

            if( $ts gt $last_ts ) {
                $reason = 'Updated since last scan';
                $do_scan = 1;

            } elsif( ! defined $info->{ filesize }) {
                $reason = "We don't know the filesize";
                $do_scan = 1;

            } elsif( $options->{ context }->{stat}->[7] != $info->{ filesize }) {
                $reason = "Filesize changed";
                $do_scan = 1;
            }

            #if( $do_scan ) {
            #    $self->msg->("rescan,modified ($last_ts / $ts),$info->{filename}");
            #};
        } else {
            # ... we have no stat info, so the file doesn't exist on disk
            # (or we are doing a rescan from the DB)
            # So, let's queue a rescan here?!
            $reason = 'No stat entry';
            $do_scan = 1;
        }
    }

    return wantarray ? ($do_scan, $reason) : $do_scan;
}

sub update_properties( $self, $info, %options ) {
    my $last_ts = $options{ last_ts } = $info->{last_scanned} // '';
    my $dry_run = exists $options{ dry_run }
                  ? delete $options{ dry_run }
                  : $self->dry_run;

    if( $info->{filename}->value !~ m!^/! ) {
        $info->{filename} = $self->store->_inflate_filename( $info->{mountpoint}, $info->{filename} );
        #warn "Made filename absolute " . $info->{filename}->value;
    };

    # This would be a kind of plugin system, maybe?!
    my $do_scan = $self->_wants_rescan( $info, \%options );

    if( $info->{filename}->value !~ m!^/! ) {
        croak sprintf "Relative filename encountered: '%s'", $info->{filename}->value;
    };

    # A callback can add more data that we then use to do more scanning
    # for example the mime_type is used subsequently for more scanning
    while( $do_scan ) {
        $do_scan = 0;
        my @updaters = _applicable_properties( \%file_properties, $info, \%options );
        my $status = $self->status;
        for my $up (@updaters) {
            my( $vis, $cb ) = @$up;
            if( $dry_run ) {
                $self->msg->( sprintf "rescan,%s,%s", $vis, $info->{filename}->value);
            } else {
                # Just so we always have a last_scanned entry:
                $info->{last_scanned} //= timestamp;
                # XXX Dummy values in the callback
                $status->( $vis, $info->{filename}->value, {}, [] );
                if( $cb->($self, $info)) {
                    $info->{last_scanned} = timestamp;
                    $do_scan = 1;
                #} else {
                #    $self->msg->( "no_change,$vis,$info->{filename}");
                }
            };
        };
    }

    # If we changed anything, update the database:
    if( ! $last_ts or ($info->{last_scanned} // '' ) ne $last_ts ) {
        #msg( sprintf "% 8s | %s", 'update', $info->{filename}->value);
        no warnings 'once';
        local $Filesys::DB::FTS::Tokenizer::tokenizer_language = $info->{language};

        $info = $self->do_update($info);
    }
    return $info
}

sub basic_direntry_info( $self, $ent, $fn, $context={ stat => [stat($fn)] }, $defaults={} ) {
    my $entry_type = $defaults->{entry_type}
                     // (-f $fn ? 'file' :
                         -d $fn ? 'directory' : undef);
    my $filesize = $defaults->{filesize}
                     // $context->{stat}->[7];
    $fn = ref $fn ? $fn : Filesys::Filename->from_native( $fn ),
    return {
        entry_type => $entry_type,
        filesize => $filesize,
        %$defaults,
        #filename => $ent,
        filename => ref $fn ? $fn : Filesys::Filename->from_native( $fn ),
        #filename => $fn,
        mtime    => $context->{stat}->[9],
    }
}

sub keep_fs_entry( $self, $name ) {
    my $store = $self->store;

        # certain filenames
        # XXX this should go into some kind of config instead
    if( $name =~ m![/\\](?:(?:\.(git|cvs|config|DS_Store|~lock\.))|__MACOSX|Thumbs.db)\z!i
        # certain file extensions
        or $name =~ m!(?:\.tmp|\.part)\z! ) {
        #$self->msg->("Skipping '$name'");
        return undef
    }

    my ($mp,$fn) = $store->to_alias( $name );
    my $skip = $store->mountpoints->{$mp};
    if( grep { index( $_, $name ) == 0 } @{ $skip->{'skip-index'} || []}) {
        #$self->msg->("Skipping '$name'");
        return undef
    }

    1
}

sub do_scan( $self, %options ) {
    my $store = $self->store;
    my $directories = delete $options{ directories };
    my $dry_run = exists $options{ dry_run }
                  ? delete $options{ dry_run }
                  : $self->dry_run;
    my $status = $options{ status } // $self->status;
    my $msg    = $options{ msg }    // $self->msg;
    my $cutoff = $options{ cutoff } // $self->cutoff;
    my $lv     = $options{ level }  // $self->level;

    weaken(my $s = $self);

    # XXX do we really want to load all of this into memory?!
    state %collection;
    %collection = map {
        $_->{parent_id} => $_
    } $store->all_collections( 'directory' )->@*;

    # Maybe we want to preseed with DB results so that we get unscanned directories
    # first, or empty directories ?!
    scan_tree_bf(
        wanted => sub($name) { $s->keep_fs_entry($name ) },
        queue => $directories,
        file => sub($file,$context,$queue) {

            my $info = $store->find_direntry_by_filename( $file );
            if( ! $info) {
                my $fullname = File::Spec->rel2abs($file, $context->{parent});
                $info = $self->basic_direntry_info($file, $fullname, $context, { entry_type => 'file' });
                $info = $self->do_update( $info );
            }

            if( ! $dry_run ) {
                $info = $self->update_properties( $info, context => $context, force => $options{ force } );

                # We also want to create a relation here, with our parent directory?!
                # We also want to create a collection here, with our parent directory?!
                # We have that information in context->{parent}
                if( defined $context->{parent}) {
                    # This should always exist since we scan and create directories
                    # before scanning and creating their contents

                    # XXX how can we cache the parent here in a sensible manner?!
                    #     this will run out of memory over time
                    state %parent;
                    my $fn = ref $context->{parent} ? $context->{parent}->native : $context->{parent};
                    $parent{ $fn }
                        //= $store->find_direntry_by_filename( $fn );
                    my $parent = $parent{ $fn };

                    $collection{ $parent->{entry_id} } //= $store->insert_or_update_collection({
                        parent_id => $parent->{entry_id},
                        collection_type => 'directory',
                        title => $parent->{title} // basename($parent->{filename}->value),
                    });

                    state %memberships;
                    if( ! $memberships{ $parent->{entry_id}} ) {
                        $memberships{ $parent->{entry_id}}
                         = +{ map { $_->{entry_id} => $_ }
                                  $store->find_memberships_by_parent( 'directory', $parent->{entry_id} )->@*
                            };
                    };
                    my $membership = $memberships{ $parent->{entry_id} }->{ $info->{ entry_id }} // $store->insert_or_update_membership({
                        collection_id => $collection{ $parent->{entry_id} }->{collection_id},
                        entry_id => $info->{entry_id},
                        position => undef,
                    });
                }
            }

        },
        directory => sub( $directory, $context, $queue ) {
            my $info = $store->find_direntry_by_filename( $directory );
            if( ! $info ) {
                my $fullname = File::Spec->rel2abs($directory, $context->{parent});
                $info = $self->basic_direntry_info($directory, $fullname, $context,{ entry_type => 'directory' });
                $info = $self->do_update(
                    $info,
                );
            };

            if( (defined $lv and $context->{level} < $lv) or $context->{ stat }->[ 9 ]> $cutoff ) {
                $status->( 'scan', $directory, $context, $queue );
                return 1;
            } else {
                return 0;
            };
        },
    );
}

sub do_update( $self, $info, %options ) {
    my $dry_run = exists $options{ dry_run }
                  ? delete $options{ dry_run }
                  : $self->dry_run;
    if( $dry_run ) {
        $self->msg->( sprintf "update,%s", $info->{filename}->value );
        return $info;
    } else {
        # Do we really want to update a directory even if nothing changed?
        # we might want to update last visited, but that should be optional...
        $info = $self->store->insert_or_update_direntry($info);
        return $info
    }
};

sub do_delete( $self, $info, %options ) {
    my $dry_run = exists $options{ dry_run }
                  ? delete $options{ dry_run }
                  : $self->dry_run;
    if( $dry_run ) {
        $self->msg->( sprintf "delete,%s", $info->{filename}->{value} );
        return $info;
    } else {
        $info = $self->store->delete_direntry($info);
    }
};

sub maintain_collections( $self, %options ) {
    # find/wipe all collections created by this which were not touched
    # by a human:

    my $generator_id = $options{generator_id};
    my $query        = $options{query};
    my $visual       = $options{visual};
    my $name         = $options{name};
    my $store        = $self->store;
    my $wipe         = $options{wipe};

    croak "Need a cluster name in the 'name' parameter"
        unless defined $name;

    my $touched = $store->selectall_named(<<'', $generator_id);
    with generated as (
       select fm.collection_id
         from filesystem_membership fm
        where fm.generator_id = :generator_id
    )
    , touched as (
       select distinct fm.collection_id
         from filesystem_membership fm
         join generated g on fm.collection_id = g.collection_id
        where fm.generator_id is null
           or fm.generator_id != :generator_id
    )
    select * from touched

    # Hrrr - we don't want to wipe the whole collection id, but only the
    # elements in that collection that were added by _this_ generator
    my $generated = $store->selectall_named(<<'', $generator_id);
    with generated as (
       select collection_id
         from filesystem_membership fm
        where fm.generator_id = :generator_id
    )
    select * from generated

    if( $wipe ) {
        my $generated = $store->selectall_named(<<'', $generator_id);
            delete
              from filesystem_membership
             where generator_id = :generator_id

        $generated = $store->selectall_named(<<'', $generator_id);
            delete
              from filesystem_collection
             where generator_id = :generator_id

    }

    # Revisit the collections and delete the empty collections

    # Find the set of collections that the queries describe:
    my $collections = $store->selectall_named( $query );

    my %collections;

    for my $rel ( $collections->@* ) {
        # Update the connection via name and generator id:
        my $collection_title = $rel->{collection_title};
        $collection_title = decode('UTF-8', $collection_title);
        next unless defined $collection_title; # NULLs don't get added

        if( ! $collections{ $collection_title }) {
            my $exists = $store->selectall_named(<<'', $collection_title, $generator_id );
                select collection_id
                     , collection_json
                  from filesystem_collection
                 where generator_id = :generator_id
                   and title = :collection_title

            if( ! $exists->@* ) {
                # create the collection
                $self->msg->(sprintf "%s: Creating %s '%s'", $generator_id, $name, $collection_title);
                $collections{ $collection_title } = $store->insert_or_update_collection({
                    generator_id => $generator_id,
                    title => $collection_title,
                    cluster_visual => $visual,
                    cluster_name => $name,
                    # well, this should come from the query, no?!
                    collection_type => 'documents',
                });

            } else {
                $self->msg->(sprintf "%s: Have %s '%s'", $generator_id, $name, $collection_title);
                $collections{ $collection_title } = $store->find_collection( $exists->[0]->{collection_id});
            }
        }

        # XXX Wipe existing membership, if it is different

        # Create new membership
                warn "->insert_or_update_membership";
        $store->insert_or_update_membership({
            collection_id => $collections{ $collection_title }->{collection_id},
            entry_id => 0+$rel->{entry_id},
            generator_id => $generator_id, # well, we shouldn't clobber the manual relations...
            # Position also from $rel, if it exists
        });

    }

    # Check collections that were created by us some time,
    # but don't exist anymore. Delete these if they were not touched by
    # a human hand
}

sub maintain_collection_images( $self, %options ) {
    # for all "directory" collections without image, if they only contain a
    # single image, use that image as the representative image of the collection

    my $store        = delete $options{ store } // $self->store;

    my $no_image = $store->execute_named(<<'');
    with no_image as (
       select c.collection_id
         from filesystem_collection c
        where c.image is null
          and c.collection_type = 'directory' -- do we need that?!
    )
    , single_image as (
       select c.collection_id as collection_id
            , count(*) as images
            , max(e.entry_id) as image
         from no_image c
         join filesystem_membership m on c.collection_id = m.collection_id
         join filesystem_entry e on m.entry_id = e.entry_id
        where e.mime_type like 'image/%'
     group by c.collection_id
       having images = 1
    )
    , debug as (
      select c.collection_id
           , c.title
           , e.entry_id
           , e.filename
        from filesystem_collection c
        join single_image si on si.collection_id = c.collection_id
        join filesystem_entry e on e.entry_id = si.image
    order by c.collection_id asc
    )
    --, debug_json as (
    --  select c.collection_id
    --       , c.title
    --       , e.filename
    --       , json_set( c.collection_json, '$.image', e.entry_id ) as collection_json
    --    from filesystem_collection c
    --    join single_image si on si.collection_id = c.collection_id
    --    join filesystem_entry e on e.entry_id = si.image
    --order by c.collection_id asc
    --)
    --select * from debug
    update filesystem_collection
       set collection_json = json_set( filesystem_collection.collection_json, '$.image', si.image )
      from (select collection_id, image from single_image) as si
     where filesystem_collection.image is null
       and filesystem_collection.collection_id = si.collection_id

    say DBIx::RunSQL->format_results( sth => $no_image );
}

1;

# Should we have a trigger that removes the image from a collection if the
# image entry gets deleted?! Or should that also simply be an operation
# left join filesystem_collection on filesystem_entry where image is not null
# and e.entry_id is null
