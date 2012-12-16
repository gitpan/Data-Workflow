package Data::Workflow::Namespace::path;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# path Namespace
# Represents a path in local filesystem
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
# use File::Basename;
use File::stat;
# use File::Path;
use Data::Workflow::Util qw(clean_path);
use Data::Workflow::Namespace;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Data::Workflow::Namespace);

@AUTO_ATTRIBUTES=qw(_path suffix_autoconvert);
@OTHER_ATTRIBUTES=qw(path);
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=(modtime_autoset=>0,suffix_autoconvert=>1);
Class::AutoClass::declare(__PACKAGE__);

sub path {
  my $self=shift;
  @_? $self->_path(clean_path($_[0])): $self->_path;
}
sub full_path {
  my($self,$resource,$version)=@_;
  my $version_id=$self->versionable? $version->id: undef;
  my $full_path=join('/',grep {length($_)} ($self->path,$version_id,$resource->relative_id));
  $full_path;
}
*full_id=\&full_path;

sub use_paths {
  my($self,$resource,$version)=@_;
  my $full_path=$self->full_path($resource,$version);
  my @paths;
  if (-f $full_path) {		# if file, full_path is all there is
    @paths=($full_path);
  } else {			# use everything in dir
    my $dh;
    if (opendir($dh,$full_path)) {
      @paths=map {"$full_path/$_"} grep !/^\./,readdir($dh);
    }
  }
  wantarray? @paths: \@paths;
}
sub use_path {
  my $self=shift;
  my @paths=$self->use_paths(@_);
  @paths==1? $paths[0]: undef;	# return undef unless exactly 1 path available
}

# uses modtime of file. ignores attempt to set 
sub modtime {
  my($self,$resource,$version)=splice(@_,0,3);
  my $full_path=$self->full_path($resource,$version);
  my($unused,$unused,$stat)=
    Data::Workflow::Util::choose_file($full_path,$self->suffix_autoconvert||0);
  my $modtime=$stat? $stat->mtime: 0;
  $version->_modtime($modtime);	# set _modtime for debugging purposes
}

# NG 08-09-23: refactored. main logic moved to Data::Workflow::Util
# TODO: add tests; improve error reporting
sub open {
  my($self,$resource,$version,$mode,$create_paths)=@_;
  Data::Workflow::Util::open_file($mode,$version->full_id,$create_paths);
}

__PACKAGE__->needs_rv(qw(full_path full_id use_path use_paths open)); # used in base class
1;
