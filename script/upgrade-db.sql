#!perl
use 5.020;
use feature 'signatures';
no warnings 'experimental::signatures';

use DBI;
use Getopt::Long;
use File::Temp 'tempfile';
use POSIX 'strftime';
use DBIx::RunSQL;
use Filesys::DB::FTS::Tokenizer;

GetOptions(
    'schema|f=s' => \my $schema_file,
);

sub migrate_db( $schema_def, $db_file ) {
    my( $fh, $tempname ) = tempfile();
    close $fh;

    my $new_dbh = DBIx::RunSQL->create(
        dsn     => "dbi:SQLite:dbname=$tempname",
        sql     => $schema_def,
        force   => 1,
        verbose => 0,
    );
    $new_dbh->disconnect;

    my $dbh = DBI->connect(
        "dbi:SQLite:dbname=:memory:",
        undef,
        undef,
        { RaiseError => 1, PrintError => 0 },
    );

    $dbh->do("attach database '$tempname' as new");
    $dbh->do("attach database '$db_file' as old");
    
    DBIx::RunSQL->run_sql_file(
        dbh => $dbh,
        fh => \*DATA,
    );

    $dbh->disconnect;

    my $timestamp = strftime '%Y%m%d-%H%M%S', localtime;
    (my $backupname = $db_file) =~ s!\.!.$timestamp.!;
    rename $db_file => $backupname or die "Can't rename $db_file to $backupname: $!";
    rename $tempname => $db_file or die "Can't rename $tempname to $db_file: $!";
}

for my $f (@ARGV) {
    migrate_db( $schema_file => $f );
}

__DATA__
insert into new.filesystem_entry      select entry_json, entry_id           from old.filesystem_entry;
insert into new.filesystem_relation   select relation_json                  from old.filesystem_relation;
insert into new.filesystem_collection select collection_json, collection_id from old.filesystem_collection;
insert into new.filesystem_membership select membership_json                from old.filesystem_membership;
