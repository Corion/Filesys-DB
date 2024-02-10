package Filesys::DB::FTS::Tokenizer;
use 5.020;
use utf8; # does that help?
use experimental 'signatures';

our $VERSION = '0.01';

use Encode;
use Lingua::Stem;
use Lingua::Stem::Cistem;
use Lingua::Stem::Es;
use Text::Unidecode;

use DBD::SQLite 1.71; # actually, our patched 1.71_06
use DBD::SQLite::Constants ':fts5_tokenizer';
use locale;
our $tokenizer_language;
our $thesaurus;

require bytes; # we need to look at byte counts of UTF-8 encoded strings

sub get_stemmer( $language ) {
    if( not defined $language
        or $language =~ /^(et|el|fi)$/ # unsupported languages...
      ) {
        # no stemmer
        return sub(@terms) { @terms };

    } elsif( fc $language eq fc 'de' ) {
        return \&Lingua::Stem::Cistem::stem;

    } elsif( fc $language eq fc 'es' ) {
        return sub( @terms ) {
            state $tmp = Lingua::Stem::Es::stem_caching({-level => 2 });
            my $stems = Lingua::Stem::Es::stem({
                -words => \@terms,
                -locale => 'es',
            });
            return @$stems;
        };

    } else {
        state $stem //= Lingua::Stem->new();
        $stem->stem_caching({ -level => 2 });
        $stem->set_locale( $tokenizer_language );
        return sub( @terms ) {
            my $stems = $stem->stem( @terms );
            return @$stems;
        };
    }
}

# Another horrible idea: Parsing the text in an HTML file (hopefully
# generated by Tika) using a regular expression
# We convert the HTML to ['text',startpos,endpos]
sub locale_tika_tokenizer { # see also: Search::Tokenizer
    return sub( $ctx, $string, $tokenizer_context_flags ) {
    #Encode::_utf8_on($string); # we assume we get UTF-8 from SQLite
    Encode::decode('UTF-8',$string); # we assume we get UTF-8 from SQLite

    my $stemmer = get_stemmer( $tokenizer_language );
    #warn sprintf "%04x", $tokenizer_context_flags;

    # We want Unicode regex match semantics, but we need to pass the offsets
    # in bytes to SQLite. Hence we use bytes::length to get the byte offsets.

    my @res;
    # Find next non-tag chunk:
    my $start_ofs;
    while( $string =~ /(?:^|>)([^<>]*)(?:<|$)/g ) {
        # Extract tokens from that part
        #$start_ofs = $-[1];
        $start_ofs = bytes::length($`); # offset of our string, as represented by length of stuff before our string
        my $run = $1;
        while( $run =~ /([^\pP\s]+)/g ) {
            # push @res, [$1,$start_ofs+$-[0], $+[0]];
            my ($start, $end) = (bytes::length($`), bytes::length($`)+bytes::length($1)+1);
            #my $term = substr($string, $start, my $len = $end-$start);
            my $term = "$1";

            $start += $start_ofs;
            $end   += $start_ofs;

            # say sprintf "%s <%s>", $term, substr( $string, $start, $end-$start);

            my $flags = 0;
            DBD::SQLite::db::fts5_xToken($ctx,$flags,lc $term,$start,$end);

            my @collocated = $stemmer->($term);

            # also provide the flattened version
            # do we really want that?! Only when indexing, not when querying!
            if($tokenizer_context_flags & FTS5_TOKENIZE_DOCUMENT) {
                my $flat = lc unidecode($term);
                if( fc $flat ne fc $term ) {
                    push @collocated, lc $flat;
                }
            }

            # also push synonyms here
            if( $thesaurus and my $synonyms = $thesaurus->dictionary->{ lc $term } ) {
                #warn sprintf "%s -> %s", lc $term, join ", ", @$synonyms;
                push @collocated, $stemmer->($synonyms->@*);
            }

            $flags = FTS5_TOKEN_COLOCATED;
            for my $t (@collocated) {
                if( fc $t ne fc $term ) {
                    #warn "Query for $term; $t" if $tokenizer_context_flags == FTS5_TOKENIZE_QUERY;
                    #warn "Index for $term; $t" if $tokenizer_context_flags == FTS5_TOKENIZE_DOCUMENT;
                    DBD::SQLite::db::fts5_xToken($ctx,$flags,$t,$start,$end);
                }
            }

        }
    }
  };
}

1;
