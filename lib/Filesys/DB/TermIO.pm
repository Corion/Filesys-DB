package Filesys::DB::TermIO;
use 5.020;

use experimental 'signatures';

our $VERSION = '0.01';

use Exporter 'import';
our @EXPORT_OK = (qw(status msg));

# Maybe this should move into its own, tiny, tiny module?!
# or we should bring Term::Output::List up to date/onto CPAN

our $last;
our $colcount;

state $use_tput = `tput cols`;
state $is_tty = -t STDOUT;

sub get_colcount() {
    if( $use_tput ) {
        $SIG{WINCH} = sub {
            undef $colcount;
        };

        return 0+`tput cols`
    } elsif( $^O =~ /mswin/i ) {
        require Win32::Console;
        return [Win32::Console->new()->Size()]->[0]
    } else {
        return undef
    }
}

sub col_trunc($msg) {
    $colcount //= get_colcount();
    my $vis = $msg;
    if( length($msg) > $colcount ) {
         $vis = substr( $msg, 0, $colcount-4 ) . '...';
    }
    return $vis
}

sub status($msg) {
    return if ! $is_tty; # no status if nobody is watching
    local $|=1;
    my $rubout = "";
    if( $last ) {
        $rubout .= "\r" . col_trunc(" " x length($last)) . "\r";
    };
    my $vis = col_trunc($msg);
    print $rubout.$vis."\r";
    $last = $vis;
}

sub msg($msg) {
    my $_last = $last;
    status("");
    say $msg;
    status($_last);
}
# erase any still active status message
END { status(""); }
