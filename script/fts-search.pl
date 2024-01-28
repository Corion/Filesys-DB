#!perl
use 5.020;
#use Filter::signatures;
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

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
    'config|f=s' => \my $config_file,
);

my $search = join " ", @ARGV;

my $console_output=1;

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

my $thesaurus = Filesys::DB::FTS::Thesaurus->load('thesaurus-search.yaml');

sub query( $search, $language='en' ) {
    # XXX detect the language from the snippet? Maybe using trigrams? Or have the user select it?
    local $Filesys::DB::FTS::Tokenizer::tokenizer_language = $language;
    local $Filesys::DB::FTS::Tokenizer::thesaurus = $thesaurus;

    my $tmp_res = $store->selectall_named(<<'', $search);
        SELECT
              fts.html
            , fts.entry_id
            , highlight(filesystem_entry_fts5, 0, '<-mark->', '</-mark->') as snippet
            , fs.filename
            , fs.entry_json
        FROM filesystem_entry_fts5 fts
        JOIN filesystem_entry fs
          ON fs.entry_id = fts.entry_id
        where fts.html MATCH :search
    order by rank

    for (@$tmp_res) {
        # Strip HTML tags ?

        $_->{html} = decode('UTF-8',$_->{html});
        $_->{snippet} = decode('UTF-8',$_->{snippet});

        my $r = $store->_inflate_entry( $_ );
        $r->{snippet} = $_->{snippet};
        $_ = $r;
        $_->{title} = $_->{content}->{title};
    }

    return $tmp_res
}

my $tmp_res = query( $search );
# prepare for output
for (@$tmp_res) {
    $_->{snippet} =~ s!\A(.*?)<-mark->!left_ell($1,15)."<-mark->"!ems;
    $_->{snippet} =~ s!</-mark->(.*?)<-mark->!mid_ell($1,15)."<-mark->"!gems;
    $_->{snippet} =~+ s!</-mark->(.*?)\z!right_ell($1,15)!ems;

    if( $console_output ) {
        $_->{snippet} =~ s!</?-mark->!!g;
    }
}

my $out = Text::Table->new('entry_id','title','snippet');
$out->load(
    map { [@{$_}{qw(entry_id title snippet)}] } @{ $tmp_res }
);
print $out;

