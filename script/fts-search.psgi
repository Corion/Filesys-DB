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

use URL::FilterSet;

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
    if ( -f 'filesys-db.yaml' ) {
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

$store->dbh->do("PRAGMA journal_mode = WAL;");
$store->dbh->do("PRAGMA synchronous = normal;");
$store->dbh->do("PRAGMA temp_store = memory;");
$store->dbh->do("PRAGMA mmap_size = 30000000000;");
$store->dbh->do('PRAGMA analysis_limit=1000');

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

sub _collection_filter( $parameters, $filterset ) {

    my $count = 0;
    my $pcount = 0;
    my $filters = { $filterset->as_sql };
    my $filter_clause = join "\n",
        map { $count++;
              my $k = $_;
              my $pcount = 0;

              my $placeholders = join ", ",
                                 map {
                                   my $name = "c${count}_${pcount}";
                                   $parameters->{ '$' . $name } = $_;
                                   ":$name"
                                 } $filters->{$k}->@*;
              # XXX we trust the filter names and values here!
              # we need to filter on the allowed names and values!
              $parameters->{'$generator_id_' . $count} = $k;
              qq{ JOIN collections c$count on (0+d.entry_id = 0+c$count.entry_id and c$count.generator_id = :generator_id_$count and c$count.filter IN ($placeholders))}
        } sort keys $filters->%*;

    return $filter_clause;
}

sub _query( $search, $filterset ) {

    my $sql = <<'';
            SELECT
                  fts.html
                , 0+fts.entry_id as entry_id
                , highlight(filesystem_entry_fts5, 0, '<-mark->', '</-mark->') as snippet
                , fs.filename
                , fs.entry_json
            FROM filesystem_entry_fts5 fts
            JOIN filesystem_entry fs
            ON fs.entry_id = fts.entry_id
            where (filesystem_entry_fts5 MATCH :search)
        order by rank


    if( ! $filterset->has_filters ) {
        return $store->selectall_named($sql, $search);

    } else {

        my %parameters;
        my $filter_clause = _collection_filter( \%parameters, $filterset );
        $parameters{ '$search' } = $search;

        my $filtered = <<"";
        with documents as (
                $sql
        )
        , collections as (
            select c.title as filter
                 , c.generator_id
                 , m.entry_id
              from filesystem_collection c
              join filesystem_membership m on 0+m.collection_id = 0+c.collection_id
              join documents d on 0+d.entry_id=0+m.entry_id
        )
        select
               d.html
             , d.entry_id
             , d.snippet
             , d.filename
             , d.entry_json
          from documents d
          $filter_clause

        my $sth = $store->bind_named($filtered, \%parameters);
        $sth->execute();
        return $sth->fetchall_arrayref({});
    }
}

sub query( $search, $filters ) {
    # XXX detect the language from the snippet? Maybe using trigrams? Or have the user select it?
    local $Filesys::DB::FTS::Tokenizer::tokenizer_language = 'en';
    local $Filesys::DB::FTS::Tokenizer::thesaurus = $thesaurus;

    my $tmp_res = _query( $search, $filters );

    # prepare for output
    my $context = 30;
    for (@$tmp_res) {
        # Strip HTML tags ?

        $_->{snippet} = decode('UTF-8',$_->{snippet});

        my $r = $store->_inflate_entry( $_ );
        $r->{content}->{title} = decode('UTF-8', $r->{content}->{title});
        $r->{content}->{html} = decode('UTF-8', $r->{content}->{html});
        $r->{snippet} = $_->{snippet};
        $_ = $r;

        my @parts = split m!(?=<-mark->)!, $r->{snippet};

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

sub filters( $search, $filters, $rows ) {
    local $Filesys::DB::FTS::Tokenizer::tokenizer_language = 'en';
    local $Filesys::DB::FTS::Tokenizer::thesaurus = $thesaurus;

    # XXX detect the language from the snippet? Maybe using trigrams? Or have the user select it?

    my %parameters = ( '$search' => $search );
    my $filter_clause = _collection_filter(\%parameters, $filters );

    my $sql = <<"";
        -- we should have the FTS last, not first
        with matching_documents as (
                SELECT
                         0+fts.entry_id as entry_id
                FROM filesystem_entry_fts5 fts
                JOIN filesystem_entry fs
                  ON fs.entry_id = fts.entry_id
            where (filesystem_entry_fts5 MATCH :search)
        )
        , collections as (
            select c.title as filter
                 , c.generator_id
                 , m.entry_id
              from filesystem_collection c
              join filesystem_membership m on 0+m.collection_id = 0+c.collection_id
              join matching_documents d on 0+d.entry_id=0+m.entry_id
        )
        , filtered as (
        select
               d.entry_id
          from matching_documents d
          $filter_clause
        )
        , more_collections as (
            select c.title as filter
                 , ifnull( json_extract( c.collection_json, '\$.generator_visual' ), 'Directory') as generator_visual
                 , c.generator_id
                 , count(*) as c
              from filesystem_collection c
              join filesystem_membership m on 0+m.collection_id = 0+c.collection_id
              join filtered d on 0+d.entry_id=0+m.entry_id
              group by filter, generator_visual, c.generator_id
        )
        select more_collections.filter
             , generator_visual
             , generator_id
             , c as "count"
          from more_collections
      order by generator_visual, c desc

    my $sth = $store->bind_named($sql, \%parameters);
    $sth->execute();
    my $tmp_res = $sth->fetchall_arrayref({});

use Data::Dumper; warn Dumper $tmp_res;

    for (@$tmp_res) {
        $_->{generator_visual} = decode( 'UTF-8', $_->{generator_visual} );
        $_->{filter} = decode( 'UTF-8', $_->{filter} );
    }

    my $res = {
        implied => [],
        existing => $filters,
        refine  => [],
    };

    for (@$tmp_res) {
        if( $_->{count} == @$rows ) {
            warn "$_->{generator_visual} / '$_->{filter}' is implied ( $_->{count} )";
            push $res->{implied}->@*, $_
        } else {
            warn "$_->{generator_visual} / '$_->{filter}' refines ( $_->{count} )";
            push $res->{refine}->@*, $_
        }
    }

    return $res
}

# Can we do manual highlighting here?!
# We would need to re-tokenize and highlight things ourselves?!
sub document( $id, $search=undef ) {
    my ($res);

    if( defined $search and length $search ) {

       my $sql = <<"";
        SELECT
              fs.entry_json
            , fs.entry_id
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
              fs.entry_json
            , fs.entry_id
            , fs.html as snippet
          FROM filesystem_entry fs
         WHERE fs.sha256 = :id

        $res = $store->selectall_named($sql, $id);
    }

    if( $res ) {
        $res->[0]->{snippet} = decode('UTF-8', $res->[0]->{snippet});

        my $r = $store->_inflate_entry( $res->[0] );

        # wtf? JSON stuff encodes octets as chars?!
        $r->{content}->{title} = decode('UTF-8', $r->{content}->{title});

        $r->{snippet} = $res->[0]->{snippet};
        $r->{snippet} =~ s!<(/?)-mark->!<${1}b>!g;

        my $entry_id = 0+$r->{entry_id};
        $r->{ collections } = $store->selectall_named(<<'', $entry_id);
            select c.collection_id
                 , c.collection_json
              from filesystem_collection c
              join filesystem_membership m using (collection_id)
             where 0+m.entry_id = 0+:entry_id

        for my $c ($r->{collections}->@*) {
            $c = $store->_inflate_collection( $c );
        }

        return $r

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

            my $r = $store->_inflate_entry( $row );
            %$row = (%$r, %$row);

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
    $c->redirect_to('index.html');
    #$c->stash( query => undef, rows => undef );
    #$c->render('index');
};

sub search_page( $c ) {
    my $search = $c->param('q') // '';
    my $filter_param = $c->param('filter') // '';

    my $filterset = URL::FilterSet->from_query( $filter_param );
    my $rows = length($search) ? query( $search, $filterset ) : undef;
    my $filters = length($search) ? filters( $search, $filterset, $rows ) : undef;

    $c->stash(
        query => $search,
        rows => $rows,
        query => $search,
        filters => $filters,
        filterset => $filterset,
    );
    $c->render('index');
}

get '/index.html' => \&search_page;
post '/index.html' => \&search_page;

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

# [ ] Add combining filters (better handler than url->with_query()
#     - maybe pregenerate the filter queries completely?
#     - we can simply append the next filter parameter?!
# [ ] Add filtering from filters
# [ ] Filter on year/time (fancy collection)
# [ ] Remove trivial filters ("implied")
# with X MATCH Y as matches (
#     select distinct collection_name, collection_id where matches.contains entry
# )
# [ ] Add RSS generation (?!)
#

app->start;

__DATA__

@@_document.html.ep
% use POSIX 'strftime';
<div>
<h3><small><%= $row->{language} %></small> <a href="/doc/<%= $row->{sha256} %>?q=<%= $query %>"><%= $row->{content}->{title} // '<no title>' %></a> (<%= $row->{content}->{creator} %>)</h3>
<small id="filename"><%= strftime '%Y-%m-%d %H:%M', localtime( $row->{mtime}) %> - <a href="/dir/<%= $row->{sha256} %>" id="link_directory"><%= $row->{filename}->value %></a></small>
<div><%== $row->{snippet} // "" %></div>
</div>

@@index.html.ep
<!DOCTYPE html>
<html>
<body>
<form method="GET" action="/index.html">
<input name="q" type="text" value="<%= $query %>"/><button type="submit">Search</button>
</form>
% if( $rows ) {
%     my $last_gen = '';
%     if( $filters->{implied}->@*) {
        <h3>
%       for my $filter ($filters->{implied}->@*) {
    <%= $filter->{filter} %>
%       }
        </h3>
%     }
%     for my $filter ($filters->{refine}->@*) {
%         if( $last_gen ne $filter->{generator_visual}) {
%             $last_gen = $filter->{generator_visual};
    <h3><%= $filter->{generator_visual} %></h3>
%          }
%          my $param = url_with->query({ filter => $filterset->as_query( $filter->{generator_id}, $filter->{filter}) });
    <p><a href="<%= $param %>"><%= $filter->{filter} %></a> (<%= $filter->{count} %>)</p>
%     }
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
<a href="<%= url_for('/index.html')->query( q => $query ) %>">Back to results</a>
<p><%= $document->{filename}->value %></p>
% for my $c ($document->{collections}->@*) {
<p><%= $c->{title} %></p>
% }
<h1><%= $document->{content}->{title} %></h1>
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

