#!perl
use 5.020;
use Mojolicious::Lite;
use feature 'signatures';
no warnings 'experimental::signatures';


use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;
use Text::Table;

use Filesys::DB::FTS::Tokenizer;

#GetOptions(
#    'mountpoint|m=s' => \my $mountpoint,
#    'alias|a=s' => \my $mount_alias,
#    'config|f=s' => \my $config_file,
#);

my $config_file;
my $mount_alias;
my $mountpoint;

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
if( $config_file ) {
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

sub left_ell($str,$len) {
    #warn $str;
    if( length($str) > $len-3 ) {
        $str = '...'.substr( $str, length($str)-$len+3, $len-3 );
    }
    return $str
}

sub mid_ell($str,$len) {
    #warn $str;
    if( length($str) > $len-6 ) {
        $str = '...'.substr( $str, length($str)-$len+6, $len-6 ).'...';
    }
    return $str
}

sub right_ell($str,$len) {
    #warn $str;
    if( length($str) > $len-3 ) {
        $str = substr( $str, 0, $len-3 ).'...';
    }
    return $str
}

sub query( $search ) {
    local $Filesys::DB::FTS::Tokenizer::tokenizer_language = 'en';

    my $tmp_res = $store->selectall_named(<<'', $search);
        SELECT
              fts.html
            , fts.title
            , fts.entry_id
            , highlight(filesystem_entry_fts5, 0, '<-mark->', '</-mark->') as snippet
            , fs.filename
            , fs.sha256
        FROM filesystem_entry_fts5 fts
        JOIN filesystem_entry fs
          ON fs.entry_id = fts.entry_id
        where fts.html MATCH :search
    order by rank

    # prepare for output
    my $context = 30;
    for (@$tmp_res) {
        # Strip HTML tags ?

        my @parts = split m!(?=<-mark->)!g, $_->{snippet};

        for ( @parts ) {
            if( /<-mark->/ ) {

                s!\A(.*?)<-mark->!left_ell($1,$context)."<-mark->"!ems;
                s!</-mark->(.*?)\z!"</-mark->".right_ell($1,$context)!ems;
                s!<-mark->!<b>!;
                s!</-mark->!</b>!;


                # but how do we handle things between two matches that are short?!
                # bar</-mark>foo<-mark->baz will turn into bar</-mark>foo... ...foo<-mark->baz

            } else {
                $_ = left_ell($_, $context);
            }
        }

        $_->{snippet} = join "", @parts;

        $_->{snippet} =~ s!</-mark->(.*?)<-mark->!"</-mark->".mid_ell($1,$context)."<-mark->"!gems;
        $_->{snippet} =~ s!<(/?)-mark->!<${1}b>!g;
    }
    return $tmp_res
}

sub document( $id ) {
    my $tmp_res = $store->selectall_named(<<'', $id);
        SELECT *
        FROM filesystem_entry
        where sha256 = :id

    if( $tmp_res ) {
        return $tmp_res->[0]
    } else {
        return
    }
}

sub collections( $id ) {
    my $tmp_res = $store->selectall_named(<<'', $id);
    with containers as (
        select c.collection_id
          from filesystem_entry fs
          join filesystem_membership m on fs.entry_id = m.entry_id
          join filesystem_collection c on m.collection_id = c.collection_id
         where fs.sha256 = :id
    )
        select fs.entry_json
             , fs.entry_id
             , fs.sha256
             , fs.title
             , fs.filename
             , c.title as collection_title
             , cont.collection_id
          from containers cont
          join filesystem_collection c on cont.collection_id = c.collection_id
          join filesystem_membership m on m.collection_id = cont.collection_id
          join filesystem_entry fs on m.entry_id = fs.entry_id
         order by c.collection_id, m.position, fs.entry_id

    if( $tmp_res ) {

        # Re-munge the result
        my @res;
        for my $row (@$tmp_res) {
            state $last_coll = 0;
            state $curr;
            if( $last_coll != $row->{collection_id}) {
                $curr = { %$row }; # well, not everything, but we don't care
                $curr->{entries} = [];
                push @res, $curr;
            }
            push $curr->{entries}->@*, $row;

            $last_coll = $row->{collection_id};
        }

        return \@res
    } else {
        return
    }
}

get '/' => sub( $c ) {
    $c->stash( query => undef, rows => undef );
    $c->render('index');
};

post '/index.html' => sub( $c ) {
    my $search = $c->param('q');
    my $rows = query( $search );
    $c->stash( query => $search, rows => $rows );
    $c->render('index');
};

get '/doc/:id' => sub( $c ) {
    my $document = document( $c->param('id'));
    $c->stash( document => $document );
    $c->render('doc');
};

get '/dir/:id' => sub( $c ) {
    my $document = document( $c->param('id'));
    my $collections = collections( $c->param('id'));
    $c->stash( document => $document, collections => $collections );
    $c->render('collections');
};


app->start;

__DATA__

@@index.html.ep
<!DOCTYPE html>
<html>
<body>
<form method="POST" action="/index.html">
<input name="q" type="text" value="<%= $query %>"/><button type="submit">Search</button>
</form>
% if( $rows ) {
%     for my $row (@$rows) {
<div>
<h3><a href="/doc/<%= $row->{sha256} %>"><%= $row->{title} %></a></h3>
<small id="filename"><a href="/dir/<%= $row->{sha256} %>" id="link_directory"><%= $row->{filename} %></a></small>
<div><%== $row->{snippet} %></div>
</div>
%     }
% }
</body>
</html>

@@doc.html.ep
<!DOCTYPE html>
<html>
<body>
...
<p><%= $document->{filename} %></p>
<h1><%= $document->{title} %></h1>
<div id="content">
<%== $document->{html} %>
</div>
</body>
</html>

@@collections.html.ep
<!DOCTYPE html>
<html>
<body>
% if( $collections ) {
%     for my $coll (@$collections) {
<h2><%= $coll->{collection_title} %></h2>
%         for my $entry ($coll->{entries}->@*) {
<div>
<h3><a href="/doc/<%= $entry->{sha256} %>"><%= $entry->{title} %></a></h3>
<small id="filename"><a href="/dir/<%= $entry->{sha256} %>" id="link_directory"><%= $entry->{filename} %></a></small>
<div><%== $entry->{snippet} %></div>
</div>
%          }
%     }
% }
</body>
</html>

