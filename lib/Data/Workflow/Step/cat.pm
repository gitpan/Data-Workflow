package Data::Workflow::Step::cat;

#################################################################################
#
# Author:	Nat Goodman
# Created:	30Dec07
# $Id: 
#
#################################################################################
use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow::VersionMap;

use base qw(Data::Workflow::Step);
# NG 10-12-02: added skip to skip header lines
# NG 10-12-22: added skip_first to leave header line on first file
use vars qw(@AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
@AUTO_ATTRIBUTES=qw(skip_first skip);
Class::AutoClass::declare(__PACKAGE__);

sub execute {
  my($self,$pipeline,$mode)=@_;
  my $log=$pipeline->log;

  my @invs=$pipeline->get_versions(VERSION_READ,$self->inputs);
  map {printlog $log info => 'reading '.$_->full_id} @invs;
  my $outv=$pipeline->get_version(VERSION_WRITE,$self->outputs->[0]);
  my $OUTPUT=$outv->open('w','create_paths');
  printlog $log info => 'writing '.$outv->full_id;

  my $skip=$self->skip_first;
  defined $skip or $skip=$self->skip;
  for my $inv (@invs) {
    my $INPUT=$inv->open;
    if (defined $skip) {
      while ($skip--) {<$INPUT>;}
    }
    print $OUTPUT (<$INPUT>);
    close $INPUT;
    $skip=$self->skip;		# set for next file in case originally set to skip_first
  }
  close $OUTPUT;
}

1;

