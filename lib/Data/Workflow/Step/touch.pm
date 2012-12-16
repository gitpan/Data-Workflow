package Data::Workflow::Step::touch;

#################################################################################
#
# Author:	Nat Goodman
# Created:	30Dec07
# $Id: 
#
# Step subclass for step type 'touch'
# touches outputs. essentially a nop used for placeholder and collector Steps
#
#################################################################################
use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow::Util qw(out_dir);
use Data::Workflow::VersionMap;

use base qw(Data::Workflow::Step);
# use vars qw(@AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
# @AUTO_ATTRIBUTES=qw(skip_first skip);
Class::AutoClass::declare;

sub execute {
  my($self,$pipeline,$mode)=@_;
  my $log=$pipeline->log;

  my @outvs=$pipeline->get_versions(VERSION_WRITE,$self->outputs);
  # NG 11-01-17: really touch path outputs. for others, Workflow will update modtime anyway
  for my $outv (grep {$_->type eq 'path'} @outvs) {
    my $path=$outv->full_id;
    out_dir($path,'create_paths');
    printlog $log info => 'touching '.$outv->full_id;
    system("touch $path");
  }
}

1;

