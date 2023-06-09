package Filesys::DB::FTS::Tokenizer;
use 5.020;
no warnings 'experimental::signatures';
use feature 'signatures';

use Lingua::Stem;
use Lingua::Stem::Cistem;

use DBD::SQLite 1.71; # actually, our patched 1.71_06
use DBD::SQLite::Constants ':fts5_tokenizer';
use locale;
our $tokenizer_language;

sub get_stemmer( $language ) {
    if( not defined $language ) {
        # no stemmer
        return sub(@terms) { @terms };

    } elsif( fc $language eq fc 'de' ) {
        return \&Lingua::Stem::Cistem::stem;

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
    my $stemmer = get_stemmer( $tokenizer_language );

    my @res;
    # Find next non-tag chunk:
    my $start_ofs;
    while( $string =~ /(?:^|>)([^<>]*)(?:<|$)/g ) {
        # Extract tokens from that part
        $start_ofs = $-[1];
        my $run = $1;
        while( $run =~ /(\w+)/g ) {
            # push @res, [$1,$start_ofs+$-[0], $+[0]];
            my ($start, $end) = ($-[0], $+[0]);
            #my $term = substr($string, $start, my $len = $end-$start);
            my $term = "$1";
            $start += $start_ofs;
            $end   += $start_ofs;

            # say sprintf "%s <%s>", $term, substr( $string, $start, $end-$start);

            my $flags = 0;
            DBD::SQLite::db::fts5_xToken($ctx,$flags,$term,$start,$end);

            my @collocated = $stemmer->($term);

            # also push synonyms here

            $flags = FTS5_TOKEN_COLOCATED;
            for my $t (@collocated) {
                if( $t ne $term ) {
                    DBD::SQLite::db::fts5_xToken($ctx,$flags,$t,$start,$end);
                }
            }

        }
    }
  };
}

1;
