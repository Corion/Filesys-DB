#!perl
use 5.020;
use feature 'signatures';
no warnings 'experimental::signatures';

use Filesys::DB;
use Filesys::DB::Watcher;

my $store = Filesys::DB->new();
$store->init_config(
    default_config_file => 'filesys-db.yaml',
    #config_file         => $config_file,
);

my $w = Filesys::DB::Watcher->new(
    store => $store,
);

$w->watch(cb => sub($event) {
    # (re)scan $event->{path}
    say join ":", $event->{action}, $event->{path}
});