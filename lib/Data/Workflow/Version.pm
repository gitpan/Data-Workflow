package Data::Workflow::Version;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# Version class
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow::ResourcePool;
use Data::Workflow::Resource;
use Data::Workflow::Namespace;

use constant 
  {RESOURCE_UNINIT=>0,RESOURCE_EMPTY=>1,RESOURCE_BUSY=>2,RESOURCE_READY=>3,RESOURCE_RETIRED=>4,
     RESOURCE_BROKEN=>-1};
our @STATES=(RESOURCE_UNINIT,RESOURCE_EMPTY,RESOURCE_BUSY,RESOURCE_READY,RESOURCE_RETIRED,
	    RESOURCE_BROKEN);

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(id resource state checktime remote_modtime _modtime);
@OTHER_ATTRIBUTES=qw(modtime);
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=(state=>RESOURCE_UNINIT,_modtime=>0,checktime=>0);
%AUTODB=
  (-collection=>'Version',
   -keys=>qq(id string),
   -transients=>qq(verbose));
Class::AutoClass::declare(__PACKAGE__);

# modtime implemented in Namespace
#   some types explicitly maintain modtime using something like the commented out
#     code below
#   path maintain _modtime for debugging purposes, but uses file modtime as 'real' 
#     value
# sub modtime {my $self=shift @_; @_? $self->_modtime($_[0]): $self->_modtime;}

# NG 08-09-19: version object may exist when 'real world' object does not exist
#              modtime.0 means real world object exists
# TBD: implement with states

sub really_exists {$_[0]->modtime>0};

sub is_uninit {$_[0]->state==RESOURCE_UNINIT};
sub is_empty {$_[0]->state==RESOURCE_EMPTY};
sub is_busy {$_[0]->state==RESOURCE_BUSY};
sub is_ready {$_[0]->state==RESOURCE_READY};
sub is_retired {$_[0]->state==RESOURCE_RETIRED};
sub is_broken {$_[0]->state==RESOURCE_BROKEN};

sub resource_id {$_[0]->resource->id};
sub namespace {$_[0]->resource->namespace;}
sub namespace_id {$_[0]->resource->namespace_id;}
sub type {$_[0]->namespace->type}
sub modtime_autoset {$_[0]->namespace->modtime_autoset;}
#sub modtime {my $self=shift; $self->namespace->modtime($self,@_);}  # handled by AUTOLOAD

# Versions need certain behavior from their Namespaces,
# but aside from the few methods above, the available
# methods vary by subclass. 
# This AUTOLOAD provides the interface between generic
# Version and specific Namespace classes
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
  return $self->namespace->method_from_version($method,$self,@_);
}

1;
