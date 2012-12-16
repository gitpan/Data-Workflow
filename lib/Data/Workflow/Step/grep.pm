package Data::Workflow::Step::grep;

#################################################################################
#
# Author:	Nat Goodman
# Created:	24Nov10
# $Id: 
#
# Step subclass for step type 'grep'. 
#   greps input file, producing output file.  d'oh :)
#   presently only works for standard and Perl patterns (-P)
#
#################################################################################
use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow::VersionMap;
use Data::Workflow::Util qw(choose_file dezip_file);

use base qw(Data::Workflow::Step);
use vars qw(@AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
@AUTO_ATTRIBUTES=qw(pattern pattern_type);
%SYNONYMS=(pat=>'pattern',pat_type=>'pattern_type');
%DEFAULTS=(pattern_type=>'perl');
Class::AutoClass::declare(__PACKAGE__);

my %type2opt=(''=>'',std=>'',perl=>'-P');

sub execute {
  my($self,$pipeline,$mode)=@_;
  my $log=$pipeline->log;

  my $inv=$pipeline->get_version(VERSION_READ,$self->inputs);
  my $inv_path=choose_file($inv->full_id);
  my $inv_arg=dezip_file($inv_path);
  printlog $log info => "reading $inv_path";
  my $outv=$pipeline->get_version(VERSION_WRITE,$self->outputs);
  my $outv_path=$outv->full_id;
  printlog $log info => "writing $outv_path";
  my($pattern,$pattern_type)=$self->get(qw(pattern pattern_type));
  my $pattern_opt=$type2opt{lc $pattern_type};
  confess "Unrecognized pattern_type $pattern_type" unless defined $pattern_opt;

  my $grep="grep $pattern_opt '$pattern'";
  # while constructing cmd, have to place input arg in correct position
  my $cmd=($inv_arg=~/^\s*</)? 
    "$grep $inv_arg":		# input is file
      "$inv_arg $grep";		# input is pipe
  $cmd.=" > $outv_path";
  printlog $log info => "executing $cmd";
  my $errstr=`$cmd`;
  printlog $log info => "execution failed: $errstr" if $?; # non-zero status means error
}

1;

