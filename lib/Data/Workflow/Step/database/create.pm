package Data::Workflow::Step::database::create;

#################################################################################
#
# Author:  Nat Goodman
# Created: 11-04-02
# $Id: 
#
# database Step to create table or view, and (for table) optionally load with data
#################################################################################

use strict;
use Carp;
use Data::Workflow::VersionMap;
use Data::Workflow::Util qw(flatten);

use base qw(Data::Workflow::Step::database);
use vars qw(@AUTO_ATTRIBUTES);
@AUTO_ATTRIBUTES=qw(view);
Class::AutoClass::declare(__PACKAGE__);

# NG 11-05-20: added index_all. boolean. if set, create indexes for all columns
sub execute {
  my($self,$pipeline,$mode)=@_;
  my @path_invs=$self->path_invs;
  my($database_inv)=$self->database_invs;
  my($database_outv)=$self->database_outvs;
  my($log,$columns,$indexes,$unique_indexes,$index_all,$query,$view,$skip)=
    $self->get(qw(log columns indexes unique_indexes index_all query view skip));

  my $step_id=$self->step_id;
  confess "database::create Step $step_id has query but no database input" 
    if $query && !$database_inv;
  confess "database::create Step $step_id has no database output" unless $database_outv;

  $self->drop($database_outv);
  unless ($view) {
    $self->create_table($columns,$query,$database_inv,$database_outv);
    $self->load_data($skip,$database_outv,@path_invs) if @path_invs;
    $self->create_indexes($indexes,$unique_indexes,$index_all,$database_outv);
  } else {
    $self->create_view($query,$database_inv,$database_outv);
  }
}
1;
