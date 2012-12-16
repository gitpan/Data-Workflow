package Data::Workflow::Step;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# Base Step class for dependency manager.
# Steps are the glue between the dependency manager and code that actually
# does the work
#
#################################################################################

use strict;
use Carp;
use Data::Dumper;
use Class::AutoClass;
use Data::Workflow::Util qw(min max flatten clean_path file);
use Data::Workflow::VersionMap;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(step_id section_id type step_format create_paths _inputs _outputs);
@OTHER_ATTRIBUTES=qw(inputs outputs);
@CLASS_ATTRIBUTES=qw(log pipeline verbose);
%SYNONYMS=(id=>'step_id');
%DEFAULTS=(inputs=>[],outputs=>[]);
%AUTODB=
  (-collection=>'Step',
   -keys=>qq(step_id string),
   -transients=>qq(verbose));
Class::AutoClass::declare(__PACKAGE__);

use constant {EXECUTE_NORMAL=>1, EXECUTE_FORCE=>2};
our @EXPORT=qw(EXECUTE_NORMAL EXECUTE_FORCE);

# some subclasses need to transform local connectivity
sub transform {my($self,$pipeline)=@_; undef}

sub inputs {
  my $self=shift @_;
  local $SIG{__DIE__} = sub { confess(@_) };
  my $inputs=@_? $self->_inputs([map {clean_path($_)} flatten(@_)]): $self->_inputs;
  unless ($inputs) {		# victor
      return wantarray? () : [];
  }
  wantarray? @$inputs: $inputs;
}
sub outputs {
  my $self=shift @_;
  my $outputs=@_? $self->_outputs([map {clean_path($_)} flatten(@_)]): $self->_outputs;
  unless ($outputs) {		# victor
      return wantarray? () : [];
  }
  wantarray? @$outputs: $outputs;
}

# class method to execute list of Steps. 
# base behavior implemented here simply executes Steps one-by-one, according to mode.
# overridden in some subclasses
sub execute_list {
  my($class,$pipeline,$mode,@steps)=@_;
  @steps=flatten(@steps);
  my $log=$class->log($pipeline->log); # store in class for easy use by subclasses
  $class->pipeline($pipeline);
  for my $step (@steps) {
    my @inputs=$pipeline->get_versions(VERSION_READ,$step->inputs);
    # NG 08-09-19: changed VERSION_WRITE to VERSION_LATEST so new version not created
    #              if old version exists
    my @outputs=$pipeline->get_versions(VERSION_LATEST,$step->outputs);
    my $time=time;
    map {$_->checktime($time)} (@inputs,@outputs);
    my $newest_input=max(map {$_->modtime} @inputs);
    my $oldest_output=min(map {$_->modtime} @outputs);
    if ($mode==EXECUTE_FORCE || !$oldest_output || $newest_input>$oldest_output) {
      print $log "BEGIN execute ",$step->id;
      $step->execute($pipeline,$mode);
      print $log "END execute";
      my $time=time;
      my @modded_outputs = grep {$_->modtime_autoset} @outputs;
#      warn "modded_outputs (ids) are ", Dumper([map {$_->full_id} @modded_outputs]);
      map {$_->modtime($time)} @modded_outputs;
#      map {$_->modtime($time)} grep {$_->modtime_autoset} @outputs;
    } else {
      print $log "SKIP execute ",$step->id;
    }
    map {$_->put} (@inputs,@outputs); # store in database to reflect execution
  }
}
# overridden in subclasses
sub execute {
  my($self,$pipeline,$mode)=@_;
}

# NG 11-01-26: added these methods, parallel to xxx_invs, xxx_outvs
#              dunno why these weren't needed before...
# TODO: refactor to eliminate obvious redundancy
sub path_inputs {shift->_ress('r','path',@_);}
sub uri_inputs {shift->_ress('r','uri',@_);}
sub database_inputs {shift->_ress('r','database',@_);}
sub path_outputs {shift->_ress('w','path',@_);}
sub uri_outputs {shift->_ress('w','uri',@_);}
sub database_outputs {shift->_ress('w','database',@_);}
sub _ress {
  my ($self,$mode,$type,@keys)=@_;
  $mode=substr($mode,0,1);
  confess "unknown mode: '$mode'" unless grep /^$mode$/, qw(r w);
  confess "unknown type: '$type'" unless grep /^$type$/, qw(path uri database);
  my @ids=$mode eq 'r'? $self->inputs : $self->outputs;
  return wantarray? @ids : \@ids unless @keys;
  my $pattern_exact='^('.join('|',@keys).')$';
  my $pattern_loose='('.join('|',@keys).')';
  my %key2id;
  for my $id (@ids) {
    my $filename=file($id);
    my ($key) = $filename =~ /$pattern_exact/;
    if (!$key) { ($key) = $filename =~ /$pattern_loose/; }
    $key2id{$key} = $id if $key;
  }
  my $k1 = scalar @keys;
  my $k2 = keys %key2id;
  my $v1 = join(",",@keys);
  my $v2 = join(",",(keys %key2id));
  confess "_ress returning incorrect number of keys $k1 $k2 : $v1 $v2\n" if scalar @keys != keys %key2id;
  return @key2id{@keys}
}
# NG 11-01-26: moved from GDxBase::Pipeline::Step - should have done this long ago...
sub path_invs {shift->_vers('r','path',@_);}
sub uri_invs {shift->_vers('r','uri',@_);}
sub database_invs {shift->_vers('r','database',@_);}
sub path_outvs {shift->_vers('w','path',@_);}
sub uri_outvs {shift->_vers('w','uri',@_);}
sub database_outvs {shift->_vers('w','database',@_);}
sub _vers {
  my ($self,$mode,$type,@keys)=@_;
  $mode=substr($mode,0,1);
  my $v_mode={r=>VERSION_READ, w=>VERSION_WRITE}->{$mode} or confess "unknown mode '$mode'";
  confess "unknown type: '$type'" unless grep /^$type$/, qw(path uri database);
  my @ids=$mode eq 'r'? $self->inputs : $self->outputs;
  my @vers=grep {$_->type eq $type} $self->pipeline->get_versions($v_mode,@ids);
  return wantarray? @vers : \@vers unless @keys;
  my $pattern_exact='^('.join('|',@keys).')$';
  my $pattern_loose='('.join('|',@keys).')';
  my %key2ver;
  for my $ver (@vers) {
    # TODO: NG 06-10-23. pattern should be tested against resource_id, not filename
    #                    resource_id includes namespace; filename does not
    # NG 07-12-20. Changed to use method in Data::Pipeline::Step
    my $filename=file($ver->resource_id);
    my ($key) = $filename =~ /$pattern_exact/;
    if (!$key) { ($key) = $filename =~ /$pattern_loose/; }
    #print "key $key ver $ver filename $filename pattern $pattern_loose\n";
    $key2ver{$key} = $ver if $key;
  }
  my $k1 = scalar @keys;
  my $k2 = keys %key2ver;
  my $v1 = join(",",@keys);
  my $v2 = join(",",(keys %key2ver));
  confess "_vers returning incorrect number of keys $k1 $k2 : $v1 $v2\n" if scalar @keys != keys %key2ver;
  return @key2ver{@keys}
}
# NG 11-01-27: moved from GDxBase::Pipeline::Step - should have done this long ago...
#   and generalized slightly. $version can be object or string/pattern
sub get_dbh {
  my($self,$version)=@_;
  unless (UNIVERSAL::isa($version,'Data::Workflow::Version')) {
    # either no version or string or Regexp
    my @vers=($self->database_invs,$self->database_outvs);
    confess "no database inputs or outputs for step '",$self->id,"'" unless @vers;
    if ($version) {	    # string or pattern
      @vers=grep {$_->resource_id=~$version} @vers;
      confess "no database inputs or outputs for step '",$self->id,"' match '$version'"
	unless @vers;
   }
    $version=$vers[0];
  } 
  $version->connect;  # uses dbh cache to avoid extra DBI connects
}

1;
