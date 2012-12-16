use strict;
use warnings;
use Module::Build;
use DBI;

use lib qw(t);
use t::Build;			# my Module::Build subclass
my $class='t::Build';
# TODO: database name should be configurable
our $test_db='test';

my $builder = $class->new
  (module_name         => 'Data::Workflow',
   license             => 'perl',
   dist_author         => q{Nat Goodman <natg@shore.net>},
   configure_requires=>{'DBI' => 1.604,
			'Module::Build' => 0.4,
		       },
   build_requires =>   {'Test::More' => 0.88,
			'Test::Deep' => 0,
			'Exporter' => 0,
		        },
   requires =>         {'Carp' => 0,
			'Class::AutoClass' => 1.55,
			'Class::AutoDB' => 1.26,
			'Class::Singleton' => 1.4,
			'Config::IniFiles' => 2.57,
			'Data::Babel' =>1.10,
			'DBI' => 1.604,
			'File::Basename' => 0,
			'File::Spec' => 3.26,
			'Graph' => 0.91,
			'Hash::AutoHash' => 1.16,
			'Hash::AutoHash::Args' => 1.16,
			'Hash::AutoHash::MultiValued' => 1.16,
			'Hash::AutoHash::Record' => 1.16,
			'List::MoreUtils' => 0.33,
			'Template' => 2.20,
		       },
   add_to_cleanup      => [ 'Data-Workflow-*' ],
   create_makefile_pl => 'small',
   use_tap_harness => 1, 			  
   test_files => 't/workflow.*.t',

);
# not possible to run tests unless MySQL available on 'localhost', and 
#   current user has enough privileges to do everything we need. 
# the experts recommend checking such requirements here (in Build.PL).
#   if tests cannot proceed, do not create Build and exit(0).
#   automated CPAN testers will report this as status UNKNOWN 
#   in this case, the test report will also include anything we print

my $ok=1;

# NG 12-12-15: words below from Babel. probably relevant here
my $mysql_errstr=chk_mysql() and $ok=0;
print <<EOS
These tests require that DBD::mysql version 4.007 or higher be
installed, that MySQL be running on 'localhost', that the user running
the tests can access MySQL without a password, and with these
credentials, has sufficient privileges to (1) create a 'test'
database, (2) create, alter, and drop tables in the 'test' database,
(3) create and drop views, and (4) run queries and updates on the
database.

When verifying these capabilities, the test driver got the following
error message:

$mysql_errstr
EOS
  if $mysql_errstr;

exit(0) unless $ok;		# do not create Build script unless tests can run

$builder->create_build_script();

# check whether MySQl test database is accessible
# return error string if not
sub chk_mysql {
  # make sure DBD::mysql is available. doesn't work to put in prereqs because
  #  if not present, install tries to install 'DBD' which does not exist
  # eval {use DBD::mysql 4.007};
  eval "use DBD::mysql 4.007";
  return $@ if $@;

  # make sure we can talk to MySQL
  my $dbh;
  eval
    {$dbh=DBI->connect("dbi:mysql:",undef,undef,
		       {AutoCommit=>1, ChopBlanks=>1, PrintError=>0, PrintWarn=>0, Warn=>0,})};
  return $@ if $@;
  return $DBI::errstr unless $dbh;

  # try to create database if necessary, then use it
  # don't worry about create-errors: may be able to use even if can't create
  $dbh->do(qq(CREATE DATABASE IF NOT EXISTS $test_db));
  $dbh->do(qq(USE $test_db)) or return $dbh->errstr;

  # make sure we can do all necessary operations
  # create, alter, drop tables. insert, select, replace, update, select, delete
  # NG 10-11-19: ops on views needed for Babel, not AutoDB
  # NG 10-11-19: DROP tables and views if they exist
  $dbh->do(qq(DROP TABLE IF EXISTS test_table)) or return $dbh->errstr;
  $dbh->do(qq(DROP VIEW IF EXISTS test_table)) or return $dbh->errstr;
  $dbh->do(qq(DROP TABLE IF EXISTS test_view)) or return $dbh->errstr;
  $dbh->do(qq(DROP VIEW IF EXISTS test_view)) or return $dbh->errstr;

  $dbh->do(qq(CREATE TABLE test_table(xxx INT))) or return $dbh->errstr;
  $dbh->do(qq(ALTER TABLE test_table ADD COLUMN yyy INT)) or return $dbh->errstr;
  $dbh->do(qq(CREATE VIEW test_view AS SELECT * from test_table)) or return $dbh->errstr;
  # do drop at end, since we need table here
  $dbh->do(qq(INSERT INTO test_table(xxx) VALUES(123))) or return $dbh->errstr;
  $dbh->do(qq(SELECT * FROM test_table)) or return $dbh->errstr;
  $dbh->do(qq(SELECT * FROM test_view)) or return $dbh->errstr;
  $dbh->do(qq(REPLACE INTO test_table(xxx) VALUES(456))) or return $dbh->errstr;
  $dbh->do(qq(UPDATE test_table SET yyy=789 WHERE xxx=123)) or return $dbh->errstr;
  $dbh->do(qq(DELETE FROM test_table WHERE xxx=123)) or return $dbh->errstr;
  $dbh->do(qq(DROP VIEW IF EXISTS test_view)) or return $dbh->errstr;
  $dbh->do(qq(DROP TABLE IF EXISTS test_table)) or return $dbh->errstr;
  # since we made it here, we can do everything!
  undef;
}
