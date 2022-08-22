#!perl
use DBIx::RunSQL;
DBIx::RunSQL->handle_command_line('filesys-db', \@ARGV);
