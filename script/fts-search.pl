#!perl
use 5.020;
#use Filter::signatures;
use feature 'signatures';
no warnings 'experimental::signatures';

use Filesys::DB;
use DBIx::RunSQL;
use Getopt::Long;
use YAML 'LoadFile';
use PerlX::Maybe;
use Text::Table;

use Filesys::DB::FTS::Tokenizer;

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

my $tmp_res = $store->selectall_named(<<'', $search);
    SELECT html
         , title
         , entry_id
         , highlight(filesystem_entry_fts5, 0, '<-mark->', '</-mark->') as snippet
      FROM filesystem_entry_fts5
      where html MATCH :search
  order by rank

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

