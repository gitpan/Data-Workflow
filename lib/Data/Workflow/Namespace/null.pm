package Data::Workflow::Namespace::null;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# null Namespace
# Used for Resources that don't have Namespaces
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use File::stat;
use Data::Workflow::Namespace;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
#@ISA = qw(Data::Workflow::Namespace);
@ISA = qw(Data::Workflow::Namespace::path);

@AUTO_ATTRIBUTES=qw();
@OTHER_ATTRIBUTES=qw();
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=(versionable=>0,modtime_autoset=>0);
Class::AutoClass::declare(__PACKAGE__);

#sub _init_self {
#  my ($self, $class, $args) = @_;
#  return unless $class eq __PACKAGE__;
#}
#sub full_id {
#  my($self,$resource_or_version)=@_;
#  my($resource,$version)=$self->_resource_or_version($resource_or_version);
#  $resource->id;
#}

#sub full_id {
#  my($self,$resource,$version)=@_;
#  my $full_id=$resource->id;
#  if ($self->versionable) {
#    my $version_id=$version->id;
##    $full_id.='_'.$version_id if $version_id;
#    $full_id="$version_id/$full_id"if length($version_id);
#  }
#  $full_id;
#}

sub full_path {
  my($self,$resource,$version)=@_;
  my $full_path=$resource->id;
  if ($self->versionable) {
    my $version_id=$version->id;
#    $full_path.='_'.$version_id if $version_id;
    $full_path="$version_id/$full_path"if length($version_id);
  }
  $full_path;
}
*full_id=\&full_path;

# uses modtime of file if exists unless explicitly set
sub modtime {
  my($self,$resource,$version)=splice(@_,0,3);
  $version->_modtime($_[0]) if @_;
  my $full_path=$self->full_path($resource,$version);
  my $stat=stat($full_path);
  $stat? $stat->mtime: $version->_modtime || 0;
}

__PACKAGE__->needs_rv(qw(full_id full_path modtime)); # used in base class
1;
