use Test::More tests => 3;
my $id = insert_or_update_direntry({ filename => 'test' })->{entry_id};
#my $sth = $dbh->prepare('select * from filesystem_entry');
#$sth->execute;
#print DBIx::RunSQL->format_results(sth => $sth);

my $new_id = find_direntry_by_filename('test')->{entry_id};
is $id, $new_id, "We can find an existing filename";
my $reinserted = insert_or_update_direntry({ filename => 'test' })->{entry_id};
is $reinserted, $id, "We detect duplicates";
