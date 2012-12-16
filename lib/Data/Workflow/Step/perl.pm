package Data::Workflow::Step::perl;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# Step subclass for step type 'perl'
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use Data::Workflow;
use Data::Workflow::Step;
use Data::Workflow::VersionMap qw(VERSION_READ VERSION_WRITE);
use Data::Dumper;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
@ISA = qw(Data::Workflow::Step);

@AUTO_ATTRIBUTES=qw();
@OTHER_ATTRIBUTES=qw();
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=(scripts=>'perl');
%DEFAULTS=();
Class::AutoClass::declare(__PACKAGE__);

sub _init_self {
  my ($self, $class, $args) = @_;
  warn __PACKAGE__, ": keys args are ", Dumper([keys %$args]);
  return unless $class eq __PACKAGE__;
}

sub execute {
    my $self = shift;
    my ($pipeline, $mode, @stuff) = @_;
    my $status;

    warn "stuff are ", join(', ', @stuff);
    warn "keys self are ", join(', ', keys %$self);
    Carp::cluck "perl->execute returning early\n";

    warn "perl: inputs are ", join(', ', $self->inputs);
    my $input = $self->inputs->[0];
    my $in_ver = $pipeline->get_version(VERSION_READ, $input) 
	or confess "no version for $input";
    my $inpath = $in_ver->full_id or confess "no full_id for $in_ver";

    warn "perl: outputs are ", join(', ', $self->outputs);
    my $output = $self->outputs->[0];
    my $out_ver = $pipeline->get_version(VERSION_WRITE, $output) 
	or confess "no version for $output";
    my $outpath = $out_ver->full_id or confess "no full_id for $out_ver";

    do { warn "can't find $inpath"; return } unless -r $inpath;

    $status = unlink $outpath;	# just in case

    my $cmd = "gunzip $inpath";
    $status = system($cmd) >> 256;
#    warn ($status? "error during '$cmd'" : "$input gunzip'd\n");
}


1;
