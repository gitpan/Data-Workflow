package Data::Workflow::Step::untar;

#################################################################################
#
# Author:	Nat Goodman
# Created:	24Nov10
# $Id: 
#
# Step subclass for step type 'untar'. 
#   untars input file, producing output file.  d'oh :)
#   presently only works when extracting single file!
#   only available option is --exclude
#
#################################################################################
use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow::VersionMap;
use Data::Workflow::Util qw(choose_file dezip_file);

use base qw(Data::Workflow::Step);
use vars qw(@AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
@AUTO_ATTRIBUTES=qw(exclude);
%SYNONYMS=();
%DEFAULTS=();
Class::AutoClass::declare(__PACKAGE__);

sub execute {
  my($self,$pipeline,$mode)=@_;
  my $log=$pipeline->log;

  my $inv=$pipeline->get_version(VERSION_READ,$self->inputs);
  my $inv_path=choose_file($inv->full_id);
  printlog $log info => "reading $inv_path";
  my $outv=$pipeline->get_version(VERSION_WRITE,$self->outputs);
  my $outv_path=$outv->full_id;
  printlog $log info => "writing $outv_path";
  my($exclude)=$self->exclude;

  my $cmd="tar xfO $inv_path";
  $cmd.=" --exclude='$exclude'" if $exclude;
  $cmd.=" > $outv_path";
  printlog $log info => "executing $cmd";
  my $errstr=`$cmd`;
  printlog $log info => "execution failed: $errstr" if $?; # non-zero status means error
}

1;

