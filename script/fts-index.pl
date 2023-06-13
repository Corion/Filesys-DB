#!perl
use 5.020;
#use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';

use Filesys::DB;
use Filesys::DB::FTS::Tokenizer;
use Filesys::DB::FTS::Thesaurus;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;
use Text::Table;

GetOptions(
    'mountpoint|m=s' => \my $mountpoint,
    'alias|a=s' => \my $mount_alias,
    'config|f=s' => \my $config_file,
    'wipe' => \my $wipe_fts,
);

my $thesaurus = Filesys::DB::FTS::Thesaurus->load('thesaurus-ecb.yaml');

# Should we have a "rebuild complete index" option?!

my $sql = join " ", @ARGV;
$sql ||= <<'SQL';
        html is not null
    and mime_type='application/pdf'
SQL

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

my @docs = $store->_inflate_sth( $store->entries( undef, $sql ));

my $thesaurus = Filesys::DB::FTS::Thesaurus->load('thesaurus-ecb.yaml');

if( $wipe_fts ) {
    $store->dbh->do(<<'SQL');
        delete from filesystem_entry_fts5
SQL
}

# Do we want this manual indexing?
# We could simply "insert into ... select (html, title, language, entry_id) from ..."
for my $doc (@docs) {
    my( $entry_id) = $doc->{ 'entry_id' };
    my( $html) = $doc->{ content }->{ html };
    my( $title ) = $doc->{ content }->{ title };

    # XXX move language to content->language
    my $language = $doc->{language};
    local $Filesys::DB::FTS::Tokenizer::tokenizer_language = $language;

    local $Filesys::DB::FTS::Tokenizer::thesaurus = $thesaurus;

    my $tmp_res = $store->selectall_named(<<'', $entry_id )->[0];
        DELETE
          FROM filesystem_entry_fts5
         WHERE entry_id = :entry_id

    my $tmp_res = $store->selectall_named(<<'', $entry_id, $language, $html, $title )->[0];
        INSERT INTO filesystem_entry_fts5(html, title, language, entry_id)
             VALUES(:html, :title, :language, :entry_id)

}

