package Data::Workflow::Namespace;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# Base of Namespace class hierarchy
# A Namespace defines the 'root' for a family of related Resources
# There is a subclass for each type of namespace
# All the cleverness is in the subclasses
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(id versionable modtime_autoset);
@OTHER_ATTRIBUTES=qw();
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=(versionable=>1,modtime_autoset=>1);
%AUTODB=
  (-collection=>'Namespace',
   -keys=>qq(id string),
   -transients=>qq(verbose));
Class::AutoClass::declare(__PACKAGE__);

sub type {
# NG 11-01-13: let non-system classes call as class method
  my($class_or_self)=@_;
  my $class=ref $class_or_self || $class_or_self;
  my $pat=__PACKAGE__.'::(.*)';
  my($type)=$class=~/$pat$/;
  $type;
}
# overridden in uri
*scheme=\&type;
*protocol=\&scheme;

# simple case: modtime stored in Version object. overriden in some subclasses
sub modtime {
  my($self,$resource,$version)=splice(@_,0,3);
  @_? $version->_modtime($_[0]): $version->_modtime
}

# Resource and Version need behavior from their Namespaces,
# but Resource and Version are generic while Namespace has
# many subclasses.
# Resource and Version uses AUTOLOAD to bridge this gap.
# A further complication is that some methods take the
# resource or version object as an argument while others will 
# break if it is passed in.
#
# Resource and Version AUTOLOADs to the methods below which,
# in turn, dispatch to the correct actual methods.

# class method initialized in subclasses
__PACKAGE__->needs_rv(qw(modtime)); # ones needed by every subclass
sub needs_rv {
  my $class=shift @_;
  no strict 'refs';
  if (@_) {
    my @methods=(@_,keys %{ __PACKAGE__ .'::NEEDS_RV'});
    @{ $class . '::NEEDS_RV' }{@methods}=@methods;
  }
  wantarray? %{ $class . '::NEEDS_RV' }: \%{ $class . '::NEEDS_RV' };
}

sub method_from_resource {
  my($self,$method,$resource)=splice(@_,0,3);
  my $version=$resource->default_version;
  # NG 10-07-30: 'class' method no longer exists in Class::AutoClass
  # return $self->class->needs_rv->{$method}? 
  return ref($self)->needs_rv->{$method}?
    $self->$method($resource,$version,@_): $self->$method(@_);
}
sub method_from_version {
  my($self,$method,$version)=splice(@_,0,3);
  my $resource=$version->resource;
  # NG 10-07-30: 'class' method no longer exists in Class::AutoClass
  # return $self->class->needs_rv->{$method}? 
  return ref($self)->needs_rv->{$method}? 
    $self->$method($resource,$version,@_): $self->$method(@_);
}

1;
