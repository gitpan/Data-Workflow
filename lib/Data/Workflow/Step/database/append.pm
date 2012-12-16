package Data::Workflow::Step::database::append;

#################################################################################
#
# Author:  Nat Goodman
# Created: 11-04-02
# $Id: 
#
# database Step to append data to existing table
#################################################################################

use strict;
use Carp;
use Data::Workflow::VersionMap;
use Data::Workflow::Util qw(flatten);

use base qw(Data::Workflow::Step::database);
use vars qw(@AUTO_ATTRIBUTES);
@AUTO_ATTRIBUTES=qw(disable_keys);
Class::AutoClass::declare(__PACKAGE__);

sub execute {
  my ($self,$pipeline,$mode)=@_;
  my @path_invs=$self->path_invs;
  my($database_inv)=$self->database_invs;
  my($database_outv)=$self->database_outvs;
  my($log,$columns,$query,$disable_keys,$skip)=
    $self->get(qw(log columns query disable_keys skip));

  my $step_id=$self->step_id;
  confess "database::append Step $step_id has query but no database input" 
    if $query && !$database_inv;
  confess "database::append Step $step_id has no database output" unless $database_outv;

  $self->disable_keys($database_outv) if $disable_keys;
  $self->insert($columns,$query,$database_inv,$database_outv) if $query;
  $self->load_data($skip,$database_outv,@path_invs) if @path_invs;
  $self->enable_keys($database_outv) if $disable_keys;
}
1;
