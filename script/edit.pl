#!perl
use 5.020;
use DBIx::RunSQL;
use Getopt::Long;

my $sql = join " ", @ARGV;

DBIx::RunSQL->create(
    dsn => 'dbi:SQLite:dbname=db/filesys-db.sqlite',
    sql => \$sql,
);
