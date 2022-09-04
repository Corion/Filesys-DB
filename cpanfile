requires 'POSIX' => '>= 0';
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
requires 'git://github.com/Corion/DBD-SQLite.git@fts5_support'; # until there is a release with the fts5 support we want
requires 'DBIx::RunSQL';
requires 'PadWalker';
requires 'Moo' => '>= 2';

requires 'Digest::SHA' => '0';
requires 'MIME::Detect' => '0';
requires 'Music::Tag' => '0';
requires 'Apache::Tika::Server' => '0';

requires 'Lingua::Stem::Cistem'; # for German
requires 'Lingua::Stem'; # for others

