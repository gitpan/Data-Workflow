package Data::Workflow::Step::database::create_schema;

#################################################################################
#
# Author:  Nat Goodman
# Created: 11-04-17
# $Id: 
#
# database Step to create tables from schema file
#################################################################################

use strict;
use Carp;
use Data::Workflow::VersionMap;
use Data::Workflow::Util qw(flatten);

use base qw(Data::Workflow::Step::database);
use vars qw(@AUTO_ATTRIBUTES);
@AUTO_ATTRIBUTES=qw(tables);
Class::AutoClass::declare(__PACKAGE__);

sub execute {
  my($self,$pipeline,$mode)=@_;
  my($log,$tables)=$self->get(qw(log tables));
  my($schema_inv)=$self->path_invs;
  my $SCHEMA=$schema_inv->open;
  my($database_outv)=$self->database_outvs;
  my @tables=split(/\s+/,$tables);
  # process schema (.sql) file to get create table statements
  printlog $log info => 'reading '.$schema_inv->full_id;
  my %want=map {$_=>$_} @tables;
  { local $/="\n\n";
    my @creates=<$SCHEMA>;
    for my $create (@creates) {
      my($tablename)=$create=~/CREATE TABLE \`(\w+)/;
      confess "Cannot parse table from $create" unless $tablename;
      next unless $want{$tablename};
      delete $want{$tablename};
      $self->tablename($tablename); # set tablename for drop & create
      $self->drop($database_outv);
      $self->create_schema($create,$database_outv);
    }
  }
  # make sure we got 'em all
  my $want=join(',',keys %want);
  confess "Did not find tables $want in ".$schema_inv->full_id if $want;
  close $SCHEMA;
}
1;
