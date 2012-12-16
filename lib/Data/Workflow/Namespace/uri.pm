package Data::Workflow::Namespace::uri;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# uri Namespace
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
# use File::Basename;
use Data::Workflow::Util qw(clean_path);
use Data::Workflow::Namespace;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Data::Workflow::Namespace);

@AUTO_ATTRIBUTES=qw(_uri);
@OTHER_ATTRIBUTES=qw(uri);
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=(versionable=>0,modtime_autoset=>0);
Class::AutoClass::declare(__PACKAGE__);

# Namespace::uri object may represent 
# a generic scheme (eg ftp)
# a scheme:site (eg, ftp://ftp.ncbi.nih.gov),
# a path on a site (eg, ftp://ftp.ncbi.nih.gov/refseq
# 
# uri objects, by default, are not versionable.

#sub _init_self {
#  my ($self, $class, $args) = @_;
#  return unless $class eq __PACKAGE__;
#}

sub uri {
  my $self=shift;
  my $uri=@_? $self->_uri(clean_path($_[0])): $self->_uri;
  $uri || $self->id;
}
sub full_uri {
  my($self,$resource,$version)=@_;
  my $id=$self->id;
  my $uri;
  if ($id=~/^\w+:/) {	# generic scheme. Resource id is all there is
    $uri=$resource->id;
  } else {			# site or path.  similar to 'path' object
    $uri=join('/',grep {$_} ($self->uri,$resource->relative_id));
  }
  if ($self->versionable) {	# note: by default, not versionable
    my $version_id=$version->id;
    if (length($version_id)) {
      my($prefix,$path)=$uri=~/^(\w+:\/\/.*?\/)(.*)$/x;
      # parse path. pattern adapted from File::Basename
      my($dir,$base,$suffix) = ($path =~ m#^(.*/)*(.*?)(\..*)*$#s);
      $path=$dir.$base.'_'.$version_id.$suffix;
      $uri=$prefix.$path;
    }
#    $uri.='_'.$version_id if $version_id;
  }
  $uri;
}
*full_id=\&full_uri;
sub scheme {
  my($self,$resource,$version)=@_;
  my $uri=$self->full_uri($resource,$version);
  my($scheme)=$uri=~/^(\w+): /x;
  $scheme;
}
*protocol=\&scheme;
sub authority {
  my($self,$resource,$version)=@_;
  my $uri=$self->full_uri($resource,$version);
  my($site)=$uri=~/^\w+:\/\/(.*?)(\/|:|$ )/x; # NG 08-07-11: added support for port
  $site;
}
*site=\&authority;
*domain=\&authority;
*host=\&authority;
# NG 08-07-11: added port.  eg, http://mysite.com:2080
sub port {
  my($self,$resource,$version)=@_;
  my $uri=$self->full_uri($resource,$version);
  my($skip,$port)=$uri=~/^\w+:\/\/.*?(\/|:(\d+)|$ )/x; # NG 08-07-11: added support for port
  $port=80 unless $port;
  $port;
}
sub path {
  my($self,$resource,$version)=@_;
  my $uri=$self->full_uri($resource,$version);
  my($path)=$uri=~/^\w+:\/\/.*?\/(.*)$/x;
  $path;
}

# used in base class
__PACKAGE__->needs_rv(qw(full_uri full_id scheme protocol authority site domain host port path));

1;
