package Data::Workflow::Resource;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# Base Resource class
# Resources are units of data that are manipulated as a whole
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow::Util qw(clean_id flatten);
use Data::Workflow::Version;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(namespace_id relative_id default_version resource_pool
		    _id _id2version
		    verbose);
@OTHER_ATTRIBUTES=qw(modtime checktime);
%SYNONYMS=();
%DEFAULTS=(versions=>[],_id2version=>{});
%AUTODB=
  (-collection=>'Resource',
   -keys=>qq(id string),
   -transients=>qq(verbose));
Class::AutoClass::declare(__PACKAGE__);

sub _init_self {
  my ($self, $class, $args) = @_;
  return unless $class eq __PACKAGE__;
  confess "Mandatory parameter -resource_pool missing" unless $self->resource_pool;
  $self->id($args->id) if $args->id; # do it here so pool will be set
  my $default_version=$self->default_version ||
    $self->default_version(new Data::Workflow::Version(-resource=>$self));
  $self->add_version($default_version);
}
#sub id {
#  my($self)=@_;
#  join('/',grep {$_} $self->get(qw(namespace_id relative_id)));
#}
sub id {
  my $self=shift @_;
  my $id=@_? $_[0]: $self->_id;
  if (@_) {
    my($namespace_id,$relative_id);
    unless ($id=~/^\w+:/) {	# normal case
      ($namespace_id,$relative_id)=$self->split_resource_id($id);
    } else {			# id is uri
#     ($namespace_id,$relative_id)=$id=~/^(\w+:)\/(\/.*)$/; # leave one slash on relative_id 
#      ($namespace_id,$relative_id)=$id=~/^(\w+:)\/\/(.*)$/;
      my $sep;			# just so regexp will work
      ($namespace_id,$sep,$relative_id)=$id=~m#^(\w+://.*?)(/|$ )(.*?)(/|$ )$#x;
      # add generic namespace, just in case...
      my $pool=$self->resource_pool;
      $pool->add_namespace($pool->new_namespace('uri',$namespace_id));
    }
    $self->set(_id=>$id,namespace_id=>$namespace_id,relative_id=>$relative_id);
  }
  $id;
}
sub split_resource_id {
  my($self,$id)=@_;
  my $pool=$self->resource_pool;
  # find the namespace (if any) for this resource
  # the correct namespace is the one matching the longest prefix of the resource
  my $id2namespace=$pool->_id2namespace;
  my @namespace_ids=keys %$id2namespace;
  $id=clean_id($id);
  my @id_parts=split('/',$id);
  my($namespace_id,$relative_id);
  while (@id_parts) {
    $namespace_id=join('/',@id_parts);
    last if grep {$namespace_id eq $_} @namespace_ids;
    pop(@id_parts);
  }
  if (@id_parts) {		# got a match. also need part of id that sits below namespace
    ($relative_id)=$id=~/^$namespace_id\/(.*)$/;
  } else {
    ($namespace_id,$relative_id)=('',$id);
#    $namespace_id=$id;
  }
  ($namespace_id,$relative_id);
}

sub versions {
  my $self=shift @_;
  if (@_) {
    my @versions=flatten(@_);
    my $id2version=$self->_id2version({});      # start with a blank slate
    $self->add_versions(@versions);             # add new ones
  }
  my @versions=values %{$self->_id2version};
  wantarray? @versions: \@versions;
}
sub new_version {
  my($self,$id)=@_;
  #print "Resource.pm new Version $id $self\n";
  new Data::Workflow::Version(-id=>$id,-resource=>$self);
}
sub id2version {
  my $self=shift @_;
  return $self->_id2version unless @_;
  return $self->_id2version($_[0]) if 'HASH' eq ref $_[0];
  my $id=shift @_;
  @_? $self->_id2version->{$id}=$_[0]: $self->_id2version->{$id};
}
sub add_versions {
  my $self=shift @_;
  my @versions=flatten(@_);
  my @results;
  my $id2version=$self->_id2version;
  for my $version (@versions) {
    ref $version or $version=$self->new_version($version); # version can be id or object
    my $version_id=$version->id;
    next if $id2version->{$version_id}; # version already exists
    $id2version->{$version_id}=$version; 
    push(@results,$version);
  }
  wantarray? @results: \@results;
}
sub add_version {
  my($self,$version)=@_;
  ($version)=$self->add_versions($version);
  $version;
}
sub get_versions {
  my $self=shift @_;
  my $id2version=$self->_id2version;
  my @versions=grep {defined $_} @$id2version{@_};
  wantarray? @versions: \@versions;
}
sub get_version {
  my($self,$version_id)=@_;
  $self->_id2version->{$version_id};
}
sub resource {$_[0]};		# for compliance with Version
*resource_id=\&id;		# for compliance with Version
sub namespace {$_[0]->resource_pool->id2namespace($_[0]->namespace_id);}
sub type {$_[0]->namespace->type}
sub checktime {my $self=shift; $self->default_version->checktime(@_); }
sub modtime_autoset {$_[0]->namespace->modtime_autoset;}
#sub full_id {$_[0]->namespace->full_id($_[0])}                     # TRASHED
#sub modtime {my $self=shift; $self->namespace->modtime($self,@_);} # handled by AUTOLOAD

# Resources need certain behavior from their Namespaces,
# but aside from the few methods above, the available
# methods vary by subclass. 
# This AUTOLOAD provides the interface between generic
# Resource and specific Namespace classes
#
# A further complication is that some methods take the
# version object as an argument while others will break
# if it is passed in.
#
use vars qw($AUTOLOAD);
sub AUTOLOAD {
  my $self=shift;
  my $method=$AUTOLOAD;
  $method=~s/^.*:://;             # strip class qualification
  return if $method eq 'DESTROY'; # the books say you should do this
  return $self->namespace->method_from_resource($method,$self,@_);
}

1;
