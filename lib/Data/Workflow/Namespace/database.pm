package Data::Workflow::Namespace::database;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# database Namespace
# Represents a MySQL database (might work for other DBMSs but not tested)
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow::Namespace;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Data::Workflow::Namespace);

# CAUTION: can't name method 'dbh'. conflicts silently (and confusingly)
# with Class::AutoDB::Serialize::dbh !!!
@AUTO_ATTRIBUTES=qw(database dsn dbd host port user password get_dbh);
@OTHER_ATTRIBUTES=qw();
@CLASS_ATTRIBUTES=qw(verbose _dsn2dbh);
%SYNONYMS=(db_driver=>'dsn', db_user=>'user', db_pass=>'password');
%DEFAULTS=(modtime_autoset=>1, _dsn2dbh=>{});
Class::AutoClass::declare(__PACKAGE__);

sub _init_self {
  my ($self, $class, $args) = @_;
  return unless $class eq __PACKAGE__;
  # parameters are diverse to accomodate different people's preferences
  # for specifying database connections
  my $user=$self->user || $args->db_user;
  my $password=$self->password || $args->db_pass || $args->db_password;

  my $dsn=$self->dsn || $args->db_driver || $args->db_dsn;
  my($database,$dbd,$host,$port);
  if ($dsn) {                   # parse off the dbd, database, host elements
    $dsn = "DBI:$dsn" unless $dsn=~ /^dbi/i;
    ($dbd)=$dsn=~/DBI:(\w+):/i;
    ($host)=$dsn=~/host=(.*?)(;|$)/;
    ($database)=$dsn=~/database=(.*?)(;|$)/;
    ($port)=$dsn=~/port=(.*?)(;|$)/;
  } else {
    $database=$self->database || $args->db_name;
    $dbd=$self->dbd || $args->db_dbd || 'mysql';
    $host=$self->host || $args->db_host;
    $port=$self->port || $args->db_port;
    $dsn="DBI:$dbd:database=$database";
    $dsn .= ";host=$host" if $host;
    $dsn .= ";port=$port" if $port;
  }
  # put values into object
  $self->set(-database=>$database,-dsn=>$dsn,-dbd=>$dbd,
	     -host=>$host,-port=>$port,-user=>$user,-password=>$password);
}

# meant to mimic a 'use database' command in mysql -victor
sub use_database {
  my($self,$resource,$version)=@_;
  my $database=$self->database;
  if ($self->versionable) {
    my $version_id=$version->id;
    $version_id=~s/\./_/g;	# MySQL database names cannot have .'s
    $database.='_'.$version_id if length($version_id);
  }
  $database;
}

sub use_dsn {
  my($self,$resource,$version)=@_;
  my $database=$self->use_database($resource,$version);
  my($dbd,$host,$port)=$self->get(qw(dbd host port));
  my $dsn="DBI:$dbd:database=$database";
  $dsn .= ";host=$host" if $host;
  $dsn .= ";port=$port" if $port;
  $dsn;
}
*full_id=\&use_dsn;
*full_dsn=\&use_dsn;

# NG 06-04-05: connect to database. uses cached dbh to avoid DBI::connect when possible
#              based on Victor's get_dbh
# TODO: add param and code to specify 'create database if necessary'. 
#       analogous to $create_paths in path::open
sub connect {
  my ($self, $resource, $version) = @_;
  my $dsn = $self->use_dsn($resource, $version);
  my $dbh = $self->dsn2dbh($dsn);
  return $dbh if $dbh;
    
  my $user = $self->user;
  my $password = $self->password;
  $dbh = DBI->connect($dsn, $user, $password) or 
    confess "Error connection with ($dsn, $user, $password): ", DBI::errstr;
  # NG 07-06-07: changed AutoCommit from 0 to 1. 1 is the beahvior we want.
  #              previous versions of DBI didn't care, but current version
  #              raises error if underlying storage mangager can't do transactions
  $dbh->{AutoCommit}	= 1;
  $dbh->{RaiseError}	= 1;
  $self->dsn2dbh($dsn,$dbh);
  $dbh;
}
sub dsn2dbh {			# class method
  my $self=shift @_;
  return $self->_dsn2dbh unless @_;
  return $self->_dsn2dbh($_[0]) if 'HASH' eq ref $_[0];
  my $dsn=shift @_;
  @_? $self->_dsn2dbh->{$dsn}=$_[0]: $self->_dsn2dbh->{$dsn};
}

__PACKAGE__->needs_rv(qw(use_database use_dsn full_id full_dsn connect)); # used in base class
# any method that is going to be called through AUTOLOAD and has the resource and version "filled in".

1;
