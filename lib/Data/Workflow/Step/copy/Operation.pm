package Data::Workflow::Step::copy::Operation;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# Internal class used by copy::execute_list to represent one 'copy' operation
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use File::Path;
use Data::Workflow;
use Data::Workflow::Step;
use Data::Workflow::Util qw(fileparse);
use Data::Workflow::VersionMap;
use Data::Workflow::Namespace::uri;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(step input output
		    patterns _ls_pattern _perl_pattern
		   );
@OTHER_ATTRIBUTES=qw();
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=();
Class::AutoClass::declare(__PACKAGE__);

#sub _init_self {
#  my ($self, $class, $args) = @_;
#  return unless $class eq __PACKAGE__;
#}
# convert step data into Operations
sub ops {
  my($class,$pipeline,$step,$input_id,$output_ids,$patterns)=@_;
  my $outputs=$step->input2outputs($input_id) || [];
  confess "No outputs for step ".$step->id." input $input_id! Should have been caught earlier!!"
    unless @$output_ids;
  my $patterns=$step->input2patterns($input_id) || [];
  $patterns=undef unless @$patterns;

  my @ops;
  my $input=$pipeline->get_version(VERSION_READ,$input_id);
  for my $output_id (@$output_ids) {
    # NG 08-09-17: changed VERSION_WRITE to VERSION_LATEST so new version not created
    #              if old version exists
    my $output=$pipeline->get_version(VERSION_LATEST,$output_id);
    push(@ops,new $class(-step=>$step,-input=>$input,-output=>$output,-patterns=>$patterns));
  }
  wantarray? @ops: \@ops;
}
# NG 08-09-17: upgrade output version from VERSION_LATEST to VERSION_ WRITE
#              needed to fix bug in copy that caused new version to be created even 
#              if old version exists 
sub upgrade_output {
  my($self,$pipeline)=@_;
  my $pattern=$self->_ls_pattern;
  my $output_id=$self->output->resource_id;
  $self->output($pipeline->get_version(VERSION_WRITE,$output_id));
}
sub ls_pattern {
  my $self=shift;
  my $pattern=$self->_ls_pattern;
  my $patterns=$self->patterns;
  return $pattern if $pattern || !$patterns;
  for (@$patterns) {
    my($type,$pattern)=@{$_};
    return $self->_ls_pattern($pattern) if $type eq 'ls_pattern';
  }
  undef;
}
sub perl_pattern {
  my $self=shift;
  my $pattern=$self->_perl_pattern;
  my $patterns=$self->patterns;
  return $pattern if $pattern || !$patterns;
  for (@$patterns) {
    my($type,$pattern)=@{$_};
    return $self->_perl_pattern($pattern) if $type eq 'perl_pattern';
  }
  undef;
}
sub in_path {
  my($self)=@_;
  my $type=$self->input->type;
  $type eq 'uri'? $self->input->path: $self->input->full_id;
}
sub in_dir {my($dir)=$_[0]->in_fileparse; $dir}
sub in_file {my($dir,$file)=$_[0]->in_fileparse; $file}
sub in_fileparse {
  my $path=$_[0]->in_path;
  my($dir,$file)=fileparse($path);
  ($dir,$file,$path);
}
sub out_path {
  my($self)=@_;
  my $type=$self->output->type;
  $type eq 'uri'? $self->output->path: $self->output->full_id;
}
sub out_dir {my($dir)=$_[0]->out_fileparse; $dir}
sub out_file {my($dir,$file)=$_[0]->out_fileparse; $file}
sub out_fileparse {
  my $path=$_[0]->out_path;
  my($dir,$file)=fileparse($path);
  ($dir,$file,$path);
}

# methods delegated to input
sub type {$_[0]->input->type}
sub input_id {$_[0]->input->resource_id}
sub real_input {$_[0]->input->full_id}
sub scheme {
  my($self)=@_;
  $self->type eq 'uri' || $self->type eq 'database' ? $self->input->scheme: 'local';
}
*protocol=\&scheme;
sub authority {$_[0]->input->authority}
*site=\&authority; *domain=\&authority; *host=\&authority;
sub port {$_[0]->input->port}
sub database {$_[0]->input->database}
sub connect {$_[0]->input->connect}

# methods delegated to output
sub output_id {$_[0]->output->resource_id}
sub real_output {$_[0]->output->full_id}
sub out_open {my $mode=$_[1] || 'w'; $_[0]->output->open($mode)}

# methods delegated to step
sub step_id {$_[0]->step->id}
sub ftp_user {$_[0]->step->ftp_user}
sub ftp_pass {$_[0]->step->ftp_pass}
sub http_params {$_[0]->step->http_params}
sub http_realm {$_[0]->step->http_realm}
sub http_user {$_[0]->step->http_user}
sub http_pass {$_[0]->step->http_pass}
sub create_paths {$_[0]->step->create_paths}

1;
