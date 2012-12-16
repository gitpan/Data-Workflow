package Data::Workflow::VersionMap;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# VersionMap - Maps resources to versions of those resource
#
#################################################################################

use strict;
use Class::AutoClass;
use Class::AutoDB;
use Exporter qw(import);
use Carp;
use Data::Workflow::Util qw(flatten);

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(id from pool
		    _id2version);
@OTHER_ATTRIBUTES=qw(id2version);
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=(_id2version=>{});
%AUTODB=
  (-collection=>'VersionMap',
   -keys=>qq(),
   -transients=>qq(verbose));
Class::AutoClass::declare(__PACKAGE__);

# NG 08-09-17: added VERSION_LATEST to fix bug in copy that caused new version to
#              be created even if old version exists
use constant {VERSION_READ=>1, VERSION_WRITE=>2, VERSION_LATEST=>3};
our @EXPORT=qw(VERSION_READ VERSION_WRITE VERSION_LATEST);

sub _init_self {
  my ($self, $class, $args) = @_;
  return unless $class eq __PACKAGE__;
  my($from,$pool)=$self->get(qw(from pool));
  if ($from && !ref $from) {	# from is id
    confess "Cannot map -from to VersionMap unless -pool is set" unless $pool;
    $from=$pool->id2vmap($from);
    confess "Invalid VersionMap id ".$args->from." in -from" unless $from;
    $self->from($from);
  }
  $pool or $self->pool($from->pool);
}
sub id2version {
  my $self=shift @_;
  return $self->_id2version unless @_;
  return $self->_id2version($_[0]) if 'HASH' eq ref $_[0];
  my $resource_id=shift @_;
  return $self->_id2version->{$resource_id}=$_[0] if @_;
  my $version=$self->_id2version->{$resource_id};
  return $version if $version;
  # else reach into from if possible
  my $from=$self->from;
  $version=$from->id2version($resource_id) if $from;
  return $version;
}
# TODO: this is a real hack, needed to allow incremental creation of
#       versions by Pipeline::Step::copy
# NG 08-09-19: trashed $created since updates now handled here
sub get_version {
  my($self,$mode,$resource)=@_;
  confess "Invalid mode $mode: should VERSION_READ, VERSION_WRITE, or VERSION_LATEST" 
    unless grep {$mode==$_} (VERSION_READ,VERSION_WRITE,VERSION_LATEST);
  my $resource_id;
  if (ref $resource) {		# resource can be id or obj
    $resource_id=$resource->id;
  } else {
    $resource_id=$resource;
  }
  my $version=$self->_id2version->{$resource_id};
  # NG 08-09-19: version object may exist when 'real world' object does not exist
  # for WRITE, use current version even if doesn't really exist
  # for other modes, can reach into from
  if ($mode==VERSION_READ || $mode==VERSION_LATEST) {
    unless ($version && $version->really_exists) {
      # reach into from if possible
      if (my $from=$self->from) {
	my $old_version=$from->id2version($resource_id);
	$version=($old_version && $old_version->really_exists)? $old_version: $version;
      }}}
  return $version if $version;
  # else create new version
  $resource=$self->pool->id2resource($resource_id);
  confess "Invalid resource id $resource_id" unless $resource;
  $version=$resource->add_version($self->id);
  confess "Invalid version for ".$self->id unless $version;
  $self->_id2version->{$resource_id}=$version if $version;
  # NG 08-09-18: put new & changed objects. needed now to handle on-the-fly creation
  #              of WRITE versions, which in turn is needed to fix bug that caused new 
  #              version to be created even if old version exists 
  # TODO: this is a very blunt way to solve the problem
  map {$_->put} ($version,$resource,$self);
  $version;
}
sub get_versions {
  my($self,$mode)=(shift @_,shift @_);
  my @versions=grep {defined $_} map {$self->get_version($mode,$_)} @_;
  wantarray? @versions: \@versions;
}

sub fill {
  my $self=shift @_;
  my @resource_ids=grep {defined $_} map {ref $_? $_->id: $_} flatten(@_);
  my $id2version=$self->_id2version;
  my $from=$self->from;
  if ($from) {
    for my $resource_id (@resource_ids) {
      next if exists $id2version->{$resource_id};
      my $version=$from->id2version($resource_id);
      $id2version->{$resource_id}=$version if $version;
    }
  }
  my @ok=grep {$id2version->{$_}} @resource_ids;
  wantarray? @ok: \@ok;
}
sub fill_and_clip {
  my $self=shift;
  $self->fill(@_);
  $self->clip;
}
sub clip {$_[0]->from(undef);}

1;
