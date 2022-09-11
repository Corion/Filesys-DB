requires 'POSIX' => '== 1.94'; # to allow installation with 5.32 ...
requires 'File::Spec';
requires 'Getopt::Long';
requires 'Encode';
requires 'JSON';
requires 'JSON::Path';
requires 'YAML';
requires 'PerlX::Maybe';

requires 'Filter::signatures';
requires 'Carp';
requires 'DBI';
#requires 'DBD::SQLite' => '>= 1.71_06';
#requires '/home/corion/Projekte/DBD-SQLite/DBD-SQLite-1.71_06.tar.gz'; # until there is a release with the fts5 support we want
requires 'DBIx::RunSQL';
requires 'PadWalker';
requires 'Moo' => '>= 2';

requires 'Digest::SHA' => '0';
requires 'MIME::Detect' => '0';
requires 'Music::Tag' => '0';
requires 'Apache::Tika::Server' => '0';

requires 'Lingua::Stem::Cistem'; # for German
requires 'Lingua::Stem'; # for others

