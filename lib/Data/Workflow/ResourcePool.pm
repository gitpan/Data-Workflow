package Data::Workflow::ResourcePool;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# ResourcePool
# An organized collection of data resources
#
#################################################################################

use strict;
use Class::AutoClass;
use Class::AutoDB;
use Carp;
use Template;
use Config::IniFiles;
use Data::Workflow::Log;
use Data::Workflow::Util qw(clean_id flatten parse_list set_defaults blessed);
use Data::Workflow::Namespace;
use Data::Workflow::VersionMap;
use Data::Workflow::Resource;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(id cfg create_flag
		    _id2namespace _id2resource _id2vmap _log
                    verbose);
@OTHER_ATTRIBUTES=qw(log);
%SYNONYMS=();
%DEFAULTS=(_id2namespace=>{},_id2resource=>{},_id2vmap=>{});
%AUTODB=
  (-collection=>'ResourcePool',
   -keys=>qq(id string),
   -transients=>qq(verbose cfg create_flag _log));
Class::AutoClass::declare(__PACKAGE__);

# NG 11-10-05: added SYSTEM_NAMESPACES, analogous to SYSTEM_STEPS, to support
#              application-specifc Namespaces needed by interaction pipeline
our @SYSTEM_NAMESPACES=qw(connectdots database null path uri);
our %SYSTEM_NAMESPACES; @SYSTEM_NAMESPACES{@SYSTEM_NAMESPACES}=@SYSTEM_NAMESPACES;
our $NAMESPACE_CLASS='Data::Workflow::Namespace';	# base of Namespace class hierarchy

sub _init_self {
  my ($self, $class, $args) = @_;
  return unless $class eq __PACKAGE__;
  my $old_self;
  my $autodb=$self->autodb;
  # fetch object from database if possible
  my $id=$self->id;
  if ($id && $autodb->is_connected) {   
    my ($old_self)=$autodb->Class::AutoDB::Database::get(-collection=>'ResourcePool',-id=>$id);
    if ($old_self) {
      $old_self->verbose($self->verbose);
      $self=$self->{__OVERRIDE__}=$old_self;
    }}
  $self->log(null Data::Workflow::Log) unless $self->log;
  # reset these parameters if specified
  $self->parse_namespaces($args->namespaces) if $args->namespaces;
  $self->add_namespace($self->new_namespace('null','')); # add null namespace if needed
  $self->add_resources($args->resources) if $args->resources;
}
sub parse_namespaces {
  my $self=shift;
  my @sources=@_? parse_list(@_): ();
  for my $source (@sources) {
    my $template = new Template
      (RELATIVE => 1,
       ABSOLUTE => 1,
       INTERPOLATE=>1, # allow 'naked' use of $ variables
       EVAL_PERL=>1, # use of [% PERL %] blocks
      );
    my($tt_out,$cfg);
    # NG 11-01-07: coded adapted from Pipeline, which got it from Babel
    # stuff useful environment variables and dbh into stash. USER now. more later maybe
    my $stash={};
    my @envs=qw(USER); @$stash{@envs}=@ENV{@envs};
    $stash->{DBH}=$self->dbh;
    $template->process($source,$stash,\$tt_out) || 
    # $template->process($source,{},\$tt_out) || 
      confess "Template::process failed: ".$template->error();
    print "========== Here is \$tt_out ===========\n",$tt_out,"\n====================\n" 
      if $self->verbose;
    if ($tt_out) {
      open(INI,'<',\$tt_out) || confess "Cannot tie TT output string \$tt_out: $!";
      $cfg=new Config::IniFiles(-file=>*INI);
      confess "Confg::IniFiles errors: @Config::IniFiles::errors" unless $cfg;
    } else {			# input was probably empty, so make empty config
      $cfg=new Config::IniFiles();
    }
    $self->cfg($cfg);
  
    # get defaults from GLOBAL
    my $section='GLOBAL';
    my %defaults=map {$_,$cfg->val($section,$_)} $cfg->Parameters($section);

    my @namespaces;
    my @sections=$cfg->Sections;
    for my $section (@sections) {
      next if $section eq 'GLOBAL'; # GLOBAL handled above
      my %actuals=map {$_,$cfg->val($section,$_)} $cfg->Parameters($section);
      set_defaults(%actuals,%defaults);
      my $type=$actuals{'type'};
      # guess type if not set
      $type or $type='database' if grep /^db|^database/, $cfg->Parameters($section);
      $type or $type='path' if grep /^path/, $cfg->Parameters($section);
      $type or $type='uri' if grep /^(uri|url)/, $cfg->Parameters($section);
      $type or $type='connectdots' if grep /^(ctd|connectdots)/,$cfg->Parameters($section);
      confess "Cannot determine namespace type for section [$section]" unless $type;
      push(@namespaces,$self->new_namespace($type,$section,%actuals));
    }
    $self->add_namespaces(@namespaces);
  }
  wantarray? @sources: \@sources;
}

sub namespaces {
  my $self=shift @_;
  if (@_) {
    my $id2namespace=$self->_id2namespace({}); # start with a blank slate
    $self->add_namespaces(@_);	               # add new ones
  }
  my @namespaces=values %{$self->_id2namespace};
  wantarray? @namespaces: \@namespaces;
}
sub new_namespace {
  my($self,$type,$id,%params)=@_;
  # NG 11-10-05: added SYSTEM_NAMESPACES, analogous to SYSTEM_STEPS to support
  #              application-specifc Namespaces needed by interaction pipeline
  # my $namespace_class=join('::',$NAMESPACE_CLASS,$type);
  my $namespace_class=$SYSTEM_NAMESPACES{$type}? join('::',$NAMESPACE_CLASS,$type): $type;
  eval "use $namespace_class";
  if ($@) {			# 'use' failed
    confess "Unknown namespace type $type" if $@=~/^Can\'t locate/;
    confess $@;
  }
  # NG 11-01-14: pass in resource_pool. application namespace may need it
  # new $namespace_class(-id=>clean_id($id),%params);
  new $namespace_class(-id=>clean_id($id),-resource_pool=>$self,%params);
}
sub id2namespace {
  my $self=shift @_;
  return $self->_id2namespace unless @_;
  return $self->_id2namespace($_[0]) if 'HASH' eq ref $_[0];
  my $id=shift @_;
  @_? $self->_id2namespace->{$id}=$_[0]: $self->_id2namespace->{$id};
}
sub add_namespaces {
  my $self=shift @_;
  my @namespaces=flatten(@_);
  my $id2namespace=$self->_id2namespace;
  my @results;
  for my $namespace (@namespaces) { 
    my $namespace_id=$namespace->id;
    # NG 06-05-14: commented out line below to allow namespace updates
    #   next if $id2namespace->{$namespace_id}; # namespace already exists
    # NG 11-01-06: commenting out the line above causes duplicate Namespace objects to be
    #   created. This is a problem for application Namespaces, like IxnSource, which 
    #   expect the contents of their collections to be correct. Let's try to be a bit
    #   more clever.
    # $id2namespace->{$namespace_id}=$namespace;
    if (my $old_namespace=$id2namespace->{$namespace_id}) { 
      %$old_namespace=%$namespace;         # namespace exists. overwrite with new contents
      bless $old_namespace,ref $namespace; #   (gulp!!) bless into new class (real crock!!)
      $namespace=$old_namespace;	   #   and re-use old object
    }
    push(@results,$namespace);
    $id2namespace->{$namespace_id}=$namespace;
  }
  wantarray? @results: \@results;
}
sub add_namespace {
  my($self,$namespace)=@_;
  ($namespace)=$self->add_namespaces($namespace);
  $namespace;
}
sub get_namespaces {
  my $self=shift @_;
  my $id2namespace=$self->_id2namespace;
  my @namespaces=grep {defined $_} @$id2namespace{@_};
  wantarray? @namespaces: \@namespaces;
}
sub get_namespace {
  my($self,$namespace_id)=@_;
  $self->_id2namespace->{$namespace_id};
}

sub resources {
  my $self=shift @_;
  # NG 08-09-19: 'set' code below not used. trashed because persistence behavior not well-defined
  #   if (@_) {
  #     my @resources=_flatten(@_);
  #     my $id2resource=$self->_id2resource({});      # start with a blank slate
  #     $self->add_resources(@resources);             # add new ones
  #   }
  my @resources=values %{$self->_id2resource};
  wantarray? @resources: \@resources;
}
sub new_resource {
  my($self,$id)=@_;
  new Data::Workflow::Resource(-id=>$id,-resource_pool=>$self);
}

sub id2resource {
  my $self=shift @_;
  return $self->_id2resource unless @_;
  return $self->_id2resource($_[0]) if 'HASH' eq ref $_[0];
  my $id=shift @_;
  @_? $self->_id2resource->{$id}=$_[0]: $self->_id2resource->{$id};
}
# NG 08-09-19: add_resources, add_resource same as get_ methods, except objects NOT put
# TODO: refactor to eliminate redundancy
sub add_resources {
  my $self=shift @_;
  my $id2resource=$self->_id2resource;
  my @resource_ids=map {clean_id($_)} @_;
  my %id2resource;
  @id2resource{@resource_ids}=@$id2resource{@resource_ids};
  my @new_ids=grep {!$id2resource{$_}} @resource_ids;
  for my $resource_id (@new_ids) {
    my $resource=$self->id2resource($resource_id,$self->new_resource($resource_id));
    $id2resource{$resource_id}=$resource;
  }
  my @resources=@id2resource{@resource_ids};
  wantarray? @resources: \@resources;
}
sub add_resource {
  my($self,$resource_id)=@_;
  my($resource)=$self->add_resources($resource_id);
  $resource;
}

sub get_resources {
  my $self=shift @_;
  my $id2resource=$self->_id2resource;
  my @resource_ids=map {clean_id($_)} @_;
  my %id2resource;
  @id2resource{@resource_ids}=@$id2resource{@resource_ids};
  my @new_ids=grep {!$id2resource{$_}} @resource_ids;
  for my $resource_id (@new_ids) {
    my $resource=$self->id2resource($resource_id,$self->new_resource($resource_id));
    $id2resource{$resource_id}=$resource;
    $self->put_resource($resource);
  }
  $self->put_object if @new_ids; # put pool if any new resources added
  my @resources=@id2resource{@resource_ids};
  wantarray? @resources: \@resources;
}
sub get_resource {
  my($self,$resource_id)=@_;
  my($resource)=$self->get_resources($resource_id);
  $resource;
}

# utility routine to get all versions in pool
sub versions {
  my($self)=@_;
  my @versions=map {$_->versions} $self->resources;
}

sub vmaps {
  my $self=shift @_;
  if (@_) {
    my $id2vmap=$self->_id2vmap({}); # start with a blank slate
    $self->add_vmaps(@_);	     # add new ones
  }
  my @vmaps=values %{$self->_id2vmap};
  wantarray? @vmaps: \@vmaps;
}
sub new_vmap {
  my($self,$id,$from)=@_;
  print "Creating VersionMap object - $id $from $self\n";
  new Data::Workflow::VersionMap(-id=>$id,-from=>$from,-pool=>$self);
}
sub id2vmap {
  my $self=shift @_;
  return $self->_id2vmap unless @_;
  return $self->_id2vmap($_[0]) if 'HASH' eq ref $_[0];
  my $id=shift @_;
  @_? $self->_id2vmap->{$id}=$_[0]: $self->_id2vmap->{$id};
}
# use this form to add VMap objects previously created or add and create
# create several VMaps with no -from
sub add_vmaps {
  my $self=shift @_;
  my @vmaps=flatten(@_);
  my @results;
  my $id2vmap=$self->_id2vmap;
  for my $vmap (@vmaps) { 
    ref $vmap or $vmap=$self->new_vmap($vmap); # vmap can be id or object
    my $vmap_id=$vmap->id;
    next if $id2vmap->{$vmap_id}; # vmap already exists
    $id2vmap->{$vmap_id}=$vmap;
    push(@results,$vmap);
  }
  wantarray? @results: \@results;
}
# use this form to add one VMap object with specified -from
sub add_vmap {
  my($self,$vmap,$from)=@_;
  my $id2vmap=$self->_id2vmap;
  ref $vmap or $vmap=$self->new_vmap($vmap,$from); # vmap can be id or object
  my $vmap_id=$vmap->id;
  $id2vmap->{$vmap_id} or $id2vmap->{$vmap_id}=$vmap;
}
sub get_vmaps {
  my $self=shift @_;
  my $id2vmap=$self->_id2vmap;
  my @vmaps=grep {defined $_} @$id2vmap{@_};
  wantarray? @vmaps: \@vmaps;
}
sub get_vmap {
  my($self,$version_id)=@_;
  $self->_id2vmap->{$version_id};
}

sub log {
  my $self=shift;
  return $self->_log unless @_;
  return $self->_log($_[0]) if 
    blessed($_[0]) && $_[0]->isa('Data::Workflow::Log'); # use log if supplied
  $self->_log(new Data::Workflow::Log(@_)); # else, try to make log from params
}

sub resource2namespace {
  my($self,$resource)=@_;
  unless (ref $resource) {	# resource can be id or object
    my $resource_id=$resource;
    $resource=$self->id2resource($resource);
    confess "Invalid resource id $resource_id" unless $resource;
  }
  $self->id2namespace($resource->namespace_id);
}
sub version2namespace {
  my($self,$version)=@_;
  $self->id2namespace($version->resource->namespace_id);
}

#sub autodb {Class::AutoDB::Globals->instance()->autodb;}
# NG 06-05-12: code below copied from Pipeline
# TODO: refactor to eliminate duplication!
sub autodb {
  my $self=shift;
  my $autodb=Class::AutoDB::Globals->instance()->autodb;
  if (@_) {
    if (ref $_[0]) {
      confess "Invalid parameter to autodb: refs should be HASHes or ARRAYs, not ".ref $autodb
	unless 'HASH' eq ref $_[0] || 'ARRAY' eq ref $_[0];
      if ($autodb) {
	$autodb->renew(@_);
      } else {
	$autodb=new Class::AutoDB(@_);
      }
    } elsif ($autodb && defined $_[0] && !$_[0]) {	# defined but FALSE
      $autodb->disconnect;
    } else {
      $autodb or $autodb=new Class::AutoDB; # this 'new' sets global autodb
    }}
  $autodb;
}

# this is a very blunt way of putting the object
sub put {
  my($self)=@_;
  return unless $self->autodb->is_connected;
  map {$_->put} ($self->namespaces,$self->vmaps,$self->resources,$self->versions);
  $self->SUPER::put;
}
# just put the ResourcePool object -- no dependents
sub put_object {$_[0]->SUPER::put}
# put a Resource object and its versions
sub put_resource {
  my($self,$resource)=@_;
  return unless $self->autodb->is_connected;
  $resource->put;
  map {$_->put} $resource->get_versions;
}

1;
