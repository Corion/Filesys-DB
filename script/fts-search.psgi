#!perl
use 5.020;
use Mojolicious::Lite;
use feature 'signatures';
no warnings 'experimental::signatures';

use Encode 'decode';
use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;
use Text::Table;

use Filesys::DB::FTS::Tokenizer;
use Filesys::DB::FTS::Thesaurus;

#GetOptions(
#    'mountpoint|m=s' => \my $mountpoint,
#    'alias|a=s' => \my $mount_alias,
#    'config|f=s' => \my $config_file,
#);

binmode STDOUT, ':encoding(UTF-8)';
binmode STDERR, ':encoding(UTF-8)';

my $config_file;
my $mount_alias;
my $mountpoint;

my $thesaurus = Filesys::DB::FTS::Thesaurus->load('thesaurus-search.yaml');

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
    # XXX detect the language from the snippet? Maybe using trigrams? Or have the user select it?
    local $Filesys::DB::FTS::Tokenizer::tokenizer_language = 'en';
    local $Filesys::DB::FTS::Tokenizer::thesaurus = $thesaurus;

    my $tmp_res = $store->selectall_named(<<'', $search);
        SELECT
              fts.html
            , fts.title
            , fts.entry_id
            , highlight(filesystem_entry_fts5, 0, '<-mark->', '</-mark->') as snippet
            , fs.filename
            , fs.sha256
            , strftime('%Y-%m-%d %H:%M', datetime(fs.mtime, 'unixepoch')) as mtime
        FROM filesystem_entry_fts5 fts
        JOIN filesystem_entry fs
          ON fs.entry_id = fts.entry_id
        where fts.html MATCH :search
    order by rank

    # prepare for output
    my $context = 30;
    for (@$tmp_res) {
        # Strip HTML tags ?

        $_->{html} = decode('UTF-8',$_->{html});
        $_->{title} = decode('UTF-8',$_->{title});
        $_->{snippet} = decode('UTF-8',$_->{snippet});

        my @parts = split m!(?=<-mark->)!, $_->{snippet};

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

        $_->{snippet} =~ s!</-mark->(.*?)<-mark->!"</-mark->".mid_ell($1,$context)."<-mark->"!gems;
        $_->{snippet} =~ s!<(/?)-mark->!<${1}b>!g;
        $_->{snippet} = join "", @parts;

    }
    return $tmp_res
}

# Can we do manual highlighting here?!
# We would need to re-tokenize and highlight things ourselves?!
sub document( $id, $search=undef ) {
    my ($res);

    if( defined $search and length $search ) {

       my $sql = <<"";
        SELECT
              fs.*
            , highlight(filesystem_entry_fts5, 0, '<-mark->', '</-mark->') as snippet
          FROM filesystem_entry fs
          JOIN filesystem_entry_fts5 fts
            ON fs.entry_id = fts.entry_id
         WHERE fs.sha256 = :id
           AND fts.html MATCH :search

        $res = $store->selectall_named($sql, $id, $search);

    } else {
       my $sql = <<"";
        SELECT
              fs.*
            , fs.html as snippet
          FROM filesystem_entry fs
         WHERE fs.sha256 = :id

        $res = $store->selectall_named($sql, $id);
    }


    if( $res ) {
        $res->[0]->{snippet} =~ s!<(/?)-mark->!<${1}b>!g;
        $res->[0]->{snippet} = decode('UTF-8', $res->[0]->{snippet});
        return $res->[0]
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
             , strftime('%Y-%m-%d %H:%M', datetime(fs.mtime, 'unixepoch')) as mtime
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
        my $last_coll = 0;
        my $curr;
        for my $row (@$tmp_res) {
            if( $last_coll != $row->{collection_id}) {
                $curr = { %$row }; # well, not everything, but we don't care
                $curr->{entries} = [];
                push @res, $curr;
            }
            $row->{title} = decode('UTF-8', $row->{title});
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

get '/index.html' => sub( $c ) {
    my $search = $c->param('q');
    my $rows = length($search) ? query( $search ) : undef;
    $c->stash( query => $search, rows => $rows, query => $search );
    $c->render('index');
};

post '/index.html' => sub( $c ) {
    my $search = $c->param('q');
    my $rows = query( $search );
    $c->stash( query => $search, rows => $rows, query => $search );
    $c->render('index');
};

get '/doc/:id' => sub( $c ) {
    my $search = $c->param('q');
    my $document = document( $c->param('id'), $search);
    $c->stash( document => $document, query => $search );
    $c->render('doc');
};

get '/dir/:id' => sub( $c ) {
    my $search = $c->param('q');
    my $document = document( $c->param('id'));
    my $collections = collections( $c->param('id'));
    $c->stash( document => $document, collections => $collections, query => $search );
    $c->render('collections');
};

# [ ] Add filters
# [ ] Filter on language
# [ ] Filter on author
# [ ] Filter on collection
# [ ] Filter on year/time

app->start;

__DATA__

@@_document.html.ep
<div>
<h3><a href="/doc/<%= $row->{sha256} %>?q=<%= $query %>"><%= $row->{title} %></a></h3>
<small id="filename"><%= $row->{mtime} %> - <a href="/dir/<%= $row->{sha256} %>" id="link_directory"><%= $row->{filename} %></a></small>
<div><%== $row->{snippet} // "" %></div>
</div>

@@index.html.ep
<!DOCTYPE html>
<html>
<body>
<form method="POST" action="/index.html">
<input name="q" type="text" value="<%= $query %>"/><button type="submit">Search</button>
</form>
% if( $rows ) {
%     for my $row (@$rows) {
%= include '_document', row => $row, query => $query
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
<%== $document->{snippet} %>
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
%= include '_document', row => $entry, query => $query
%          }
%     }
% }
</body>
</html>

